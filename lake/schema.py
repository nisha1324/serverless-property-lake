"""Price Paid Data schema, validation rules and the curated (address-free) shape.

The monthly file has no header row; column order follows HM Land Registry's
published field list. Each row is a *change*: record_status A = added,
C = changed, D = deleted.
"""
import io

import pandas as pd

RAW_COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
    "new_build", "tenure", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category", "record_status",
]

ALLOWED = {
    "property_type": {"D", "S", "T", "F", "O"},  # detached, semi, terraced, flat, other
    "new_build": {"Y", "N"},
    "tenure": {"F", "L", "U"},  # freehold, leasehold, unknown
    "ppd_category": {"A", "B"},  # A = standard sale, B = additional (repossessions, buy-to-let…)
    "record_status": {"A", "C", "D"},
}

ID_PATTERN = r"^\{[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}\}$"
POSTCODE_PATTERN = r"^[A-Z]{1,2}[0-9][0-9A-Z]? [0-9][A-Z]{2}$"

# Columns kept in the curated zone. House number, flat, street and locality are
# dropped (data minimisation); the postcode is cut down to district + sector.
CURATED_COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "transfer_month",
    "postcode_district", "postcode_sector", "property_type", "new_build",
    "tenure", "town_city", "district", "county", "ppd_category",
    "record_status", "release",
]


def read_raw(body: bytes) -> pd.DataFrame:
    """Parse the header-less CSV, keeping every field as text for validation."""
    return pd.read_csv(
        io.BytesIO(body), header=None, names=RAW_COLUMNS, dtype=str,
        keep_default_na=False,
    )


def validate(raw: pd.DataFrame, release: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split rows into (valid, rejected). Rejected rows get a `reject_reason`.

    Rules run in order and each row is tagged with the first rule it breaks:
    1. bad_transaction_id   not a {GUID}
    2. duplicate_id         repeats an earlier row's transaction_id
    3. bad_price            not a whole number of pounds > 0
    4. bad_date             unparseable date
    5. date_after_release   transfer dated after the end of the release month
    6. bad_<field>          a code outside the published list
    """
    df = raw.copy()
    reason = pd.Series("", index=df.index, dtype=object)

    def tag(mask: pd.Series, label: str) -> None:
        reason[(reason == "") & mask] = label

    tag(~df["transaction_id"].str.match(ID_PATTERN), "bad_transaction_id")
    tag(df["transaction_id"].duplicated(keep="first"), "duplicate_id")

    price = pd.to_numeric(df["price"], errors="coerce")
    tag(price.isna() | (price <= 0) | (price % 1 != 0), "bad_price")

    date = pd.to_datetime(df["date_of_transfer"], format="%Y-%m-%d %H:%M", errors="coerce")
    tag(date.isna(), "bad_date")
    release_end = pd.Period(release, freq="M").end_time
    tag(date > release_end, "date_after_release")

    for field, allowed in ALLOWED.items():
        tag(~df[field].isin(allowed), f"bad_{field}")

    rejected = raw[reason != ""].assign(reject_reason=reason[reason != ""])
    ok = reason == ""
    valid = df[ok].assign(price=price[ok].astype("int64"), date_of_transfer=date[ok])
    return valid, rejected


def to_curated(valid: pd.DataFrame, release: str) -> pd.DataFrame:
    """Derive analysis columns and drop address detail."""
    postcode = valid["postcode"].where(valid["postcode"].str.match(POSTCODE_PATTERN))
    out = pd.DataFrame({
        "transaction_id": valid["transaction_id"],
        "price": valid["price"],
        "date_of_transfer": valid["date_of_transfer"].dt.date,
        "transfer_month": valid["date_of_transfer"].dt.strftime("%Y-%m"),
        "postcode_district": postcode.str.split(" ").str[0],
        "postcode_sector": postcode.str[:-2],
        "property_type": valid["property_type"],
        "new_build": valid["new_build"] == "Y",
        "tenure": valid["tenure"],
        "town_city": valid["town_city"],
        "district": valid["district"],
        "county": valid["county"],
        "ppd_category": valid["ppd_category"],
        "record_status": valid["record_status"],
        "release": release,
    })
    return out[CURATED_COLUMNS].reset_index(drop=True)


def quality_flags(curated: pd.DataFrame) -> dict[str, int]:
    """Rows that are kept but worth an analyst's attention."""
    return {
        "postcode_missing_or_invalid": int(curated["postcode_district"].isna().sum()),
        "price_below_10k": int((curated["price"] < 10_000).sum()),
        "price_5m_or_more": int((curated["price"] >= 5_000_000).sum()),
        "transfer_before_2020": int((curated["transfer_month"] < "2020-01").sum()),
    }

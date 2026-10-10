"""Assemble the Lambda code bundle for infra/template.yaml.

Copies only the modules the handler needs into build/function/lake/. pandas
and pyarrow come from the AWS SDK for pandas layer and boto3 from the Lambda
runtime, so the bundle has no third-party packages and stays a few KB.
The query layer, emulator helpers and scripts are not shipped.

Usage: python infra/package.py [--out build/function]
"""
import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FUNCTION_MODULES = ["__init__.py", "config.py", "schema.py", "handler.py"]


def build(out: Path) -> list[Path]:
    if out.exists():
        shutil.rmtree(out)
    pkg = out / "lake"
    pkg.mkdir(parents=True)
    copied = []
    for name in FUNCTION_MODULES:
        copied.append(Path(shutil.copy2(ROOT / "lake" / name, pkg / name)))
    return copied


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "build" / "function")
    args = parser.parse_args()
    files = build(args.out)
    size = sum(f.stat().st_size for f in files)
    print(f"wrote {len(files)} files ({size:,} bytes) to {args.out}")


if __name__ == "__main__":
    main()

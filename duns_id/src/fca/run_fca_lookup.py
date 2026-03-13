#!/usr/bin/env python3
# =============================================================================
# run_fca_lookup.py — FCA Broker Lookup pipeline
# =============================================================================
#
# Reads an input CSV of company names (or reference numbers) and queries the
# FCA Broker Query API to retrieve firm details and addresses.
#
# Two lookup modes:
#   1. BY NAME   — uses D_CUSTOMER_NAME (or another name column) to *search*
#                  the FCA register, then fetches addresses for each match.
#   2. BY FRN    — uses an existing reference_number column to skip search
#                  and go straight to the address endpoint.
#
# Outputs (saved to data/output/fca/):
#   search_results.csv   — raw search hits
#   address_results.csv  — raw address rows
#   merged_results.csv   — search + address joined on reference_number
#   errors.csv           — any failures with stage + error detail
#
# Usage:
#   # Search by company name (default)
#   python -m src.fca.run_fca_lookup
#   python -m src.fca.run_fca_lookup --input data/input/my_companies.csv --column D_CUSTOMER_NAME
#
#   # Lookup by reference number directly
#   python -m src.fca.run_fca_lookup --mode frn --column reference_number
#
#   # Custom output directory
#   python -m src.fca.run_fca_lookup --output data/output/fca_run2
# =============================================================================

import argparse
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .client import FCAClient
from .config import FCAConfig
from .parser import normalize_address_rows, normalize_search_rows


# =============================================================================
# CORE PROCESSING
# =============================================================================

def process_by_name(
    client: FCAClient,
    companies: List[str],
    delay_seconds: float,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Search the FCA register by company name, then fetch addresses."""
    all_search: List[Dict] = []
    all_address: List[Dict] = []
    errors: List[Dict] = []

    for i, company in enumerate(companies, 1):
        print(f"  [{i}/{len(companies)}] Searching: {company}")

        try:
            search_rows = client.search_firm(company_name=company, page="1")

            if not search_rows:
                errors.append({
                    "input_company": company,
                    "stage": "search",
                    "error": "No search results returned",
                })
                continue

            norm_search = normalize_search_rows(
                input_company=company,
                search_rows=search_rows,
            )
            all_search.extend(norm_search)

            for sr in norm_search:
                frn = sr.get("reference_number")
                if not frn:
                    errors.append({
                        "input_company": company,
                        "stage": "search_parse",
                        "error": "Missing reference number in search row",
                    })
                    continue

                try:
                    address_rows = client.get_address_by_frn(
                        frn=str(frn), page="1"
                    )

                    if not address_rows:
                        errors.append({
                            "input_company": company,
                            "reference_number": frn,
                            "stage": "address",
                            "error": "No address results returned",
                        })
                        continue

                    norm_addr = normalize_address_rows(
                        input_company=company,
                        reference_number=str(frn),
                        address_rows=address_rows,
                    )
                    all_address.extend(norm_addr)

                    time.sleep(delay_seconds)

                except Exception as exc:
                    errors.append({
                        "input_company": company,
                        "reference_number": frn,
                        "stage": "address",
                        "error": str(exc),
                    })

            time.sleep(delay_seconds)

        except Exception as exc:
            errors.append({
                "input_company": company,
                "stage": "search",
                "error": str(exc),
            })

    return all_search, all_address, errors


def process_by_frn(
    client: FCAClient,
    frns: List[str],
    delay_seconds: float,
) -> Tuple[List[Dict], List[Dict]]:
    """Fetch addresses directly by FCA Reference Number (skip search)."""
    all_address: List[Dict] = []
    errors: List[Dict] = []

    for i, frn in enumerate(frns, 1):
        print(f"  [{i}/{len(frns)}] Address lookup FRN: {frn}")

        try:
            address_rows = client.get_address_by_frn(frn=str(frn), page="1")

            if not address_rows:
                errors.append({
                    "reference_number": frn,
                    "stage": "address",
                    "error": "No address results returned",
                })
                continue

            norm_addr = normalize_address_rows(
                input_company="",
                reference_number=str(frn),
                address_rows=address_rows,
            )
            all_address.extend(norm_addr)

            time.sleep(delay_seconds)

        except Exception as exc:
            errors.append({
                "reference_number": frn,
                "stage": "address",
                "error": str(exc),
            })

    return all_address, errors


# =============================================================================
# OUTPUT
# =============================================================================

def save_outputs(
    search_results: List[Dict],
    address_results: List[Dict],
    errors: List[Dict],
    output_dir: str,
) -> None:
    """Write all result DataFrames to CSV."""
    os.makedirs(output_dir, exist_ok=True)

    search_df = pd.DataFrame(search_results)
    address_df = pd.DataFrame(address_results)
    errors_df = pd.DataFrame(errors)

    search_path = os.path.join(output_dir, "search_results.csv")
    address_path = os.path.join(output_dir, "address_results.csv")
    merged_path = os.path.join(output_dir, "merged_results.csv")
    errors_path = os.path.join(output_dir, "errors.csv")

    search_df.to_csv(search_path, index=False, encoding="utf-8-sig")
    address_df.to_csv(address_path, index=False, encoding="utf-8-sig")
    errors_df.to_csv(errors_path, index=False, encoding="utf-8-sig")

    # Merge search + address into one flat file
    if not search_df.empty and not address_df.empty:
        merged_df = address_df.merge(
            search_df[
                [
                    "input_company",
                    "reference_number",
                    "type_of_business_or_individual",
                    "name",
                    "status",
                ]
            ],
            on=["input_company", "reference_number"],
            how="left",
        )
        merged_df.to_csv(merged_path, index=False, encoding="utf-8-sig")
    elif not address_df.empty:
        # FRN mode — no search results to merge
        address_df.to_csv(merged_path, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(merged_path, index=False, encoding="utf-8-sig")

    print(f"\n  Saved: {search_path}  ({len(search_df):,} rows)")
    print(f"  Saved: {address_path}  ({len(address_df):,} rows)")
    print(f"  Saved: {merged_path}")
    print(f"  Saved: {errors_path}  ({len(errors_df):,} rows)")


# =============================================================================
# INPUT LOADING
# =============================================================================

def load_input_values(
    input_path: str,
    column: str,
) -> List[str]:
    """Load a CSV/Excel and return the deduplicated, non-null values from
    *column* as a list of strings."""

    path = Path(input_path)
    ext = path.suffix.lower()

    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    if column not in df.columns:
        raise ValueError(
            f"Column '{column}' not found in input file.\n"
            f"Available columns: {list(df.columns)}"
        )

    values = (
        df[column]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )

    print(f"  Loaded {len(values):,} unique values from column '{column}'")
    return values


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="FCA Broker Lookup — search firms or fetch addresses",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Path to input CSV/Excel (default: data/input/input_companies.csv)",
    )
    parser.add_argument(
        "--column",
        default="D_CUSTOMER_NAME",
        help="Column to read lookup values from (default: D_CUSTOMER_NAME)",
    )
    parser.add_argument(
        "--mode",
        choices=["name", "frn"],
        default="name",
        help="Lookup mode: 'name' searches by company name (default), "
             "'frn' fetches addresses by reference number directly",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output directory (default: data/output/fca)",
    )
    args = parser.parse_args()

    # Resolve paths relative to duns_id/ root
    project_root = Path(__file__).resolve().parents[2]

    input_path = (
        Path(args.input) if args.input else project_root / "data" / "input" / "input_companies.csv"
    )
    output_dir = (
        Path(args.output) if args.output else project_root / "data" / "output" / "fca"
    )

    print("=" * 60)
    print("  FCA BROKER LOOKUP")
    print("=" * 60)
    print(f"  Mode   : {args.mode}")
    print(f"  Input  : {input_path}")
    print(f"  Column : {args.column}")
    print(f"  Output : {output_dir}")
    print("=" * 60)

    # Load config & client
    config = FCAConfig.from_env()
    client = FCAClient(config=config)

    # Load input values
    values = load_input_values(
        input_path=str(input_path),
        column=args.column,
    )

    if not values:
        print("  No values to process. Exiting.")
        return

    # Run the appropriate mode
    if args.mode == "name":
        search_results, address_results, errors = process_by_name(
            client=client,
            companies=values,
            delay_seconds=config.request_delay_seconds,
        )
    else:
        # FRN mode — no search step
        address_results, errors = process_by_frn(
            client=client,
            frns=values,
            delay_seconds=config.request_delay_seconds,
        )
        search_results = []

    # Save
    save_outputs(
        search_results=search_results,
        address_results=address_results,
        errors=errors,
        output_dir=str(output_dir),
    )

    print("\n✅ FCA lookup complete.")


if __name__ == "__main__":
    main()

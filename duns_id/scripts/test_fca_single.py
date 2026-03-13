#!/usr/bin/env python3
# =============================================================================
# test_fca_single.py — Quick sanity test for the FCA Broker Lookup module
# =============================================================================
#
# Runs a single company name through the FCA API and prints the raw results
# so you can compare against what you see in Insomnia.
#
# Usage (run from the duns_id/ folder):
#
#   python scripts/test_fca_single.py "Barclays Bank PLC"
#   python scripts/test_fca_single.py "Barclays Bank PLC" --frn 122702
#
# Options:
#   --frn   If you already know the FCA Reference Number, skip the search
#           step and go straight to the address lookup.
#   --page  Page number for the search (default: 1)
# =============================================================================

import argparse
import json
import sys
from pathlib import Path

# Make sure we can import src.fca from this script location
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.fca.client import FCAClient
from src.fca.config import FCAConfig
from src.fca.parser import normalize_address_rows, normalize_search_rows


def print_json(label: str, data) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print("=" * 60)
    print(json.dumps(data, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FCA single-company test — compare output against Insomnia"
    )
    parser.add_argument("company", help="Company name to search (e.g. 'Barclays Bank PLC')")
    parser.add_argument("--frn", default=None, help="Known FCA Reference Number — skips search")
    parser.add_argument("--page", default="1", help="Page number for search results (default: 1)")
    args = parser.parse_args()

    # ── Load config ───────────────────────────────────────────────────────────
    try:
        config = FCAConfig.from_env()
    except ValueError as e:
        print(f"\n❌  Config error:\n{e}")
        sys.exit(1)

    print(f"\nCorrelation-ID : {config.correlation_id}")
    print(f"Channel-ID     : {config.channel_id}")

    client = FCAClient(config=config)

    # ── Step 1: Search (unless FRN provided directly) ─────────────────────────
    if args.frn:
        frn = args.frn
        print(f"\nSkipping search — using FRN directly: {frn}")
        search_rows = []
    else:
        print(f"\nSearching FCA register for: '{args.company}'  (page {args.page})")
        try:
            raw_search = client.search_firm(company_name=args.company, page=args.page)
        except Exception as e:
            print(f"\n❌  Search failed: {e}")
            sys.exit(1)

        print_json("RAW SEARCH RESPONSE (Data array)", raw_search)

        norm_search = normalize_search_rows(
            input_company=args.company,
            search_rows=raw_search,
        )
        print_json("NORMALISED SEARCH ROWS", norm_search)

        if not norm_search:
            print("\nNo results — nothing to look up.")
            return

        # Use the first result's FRN for the address lookup
        frn = norm_search[0].get("reference_number")
        if not frn:
            print("\nFirst result has no reference_number — cannot do address lookup.")
            return

        print(f"\nUsing first result FRN for address lookup: {frn}")

    # ── Step 2: Address lookup ────────────────────────────────────────────────
    print(f"\nFetching address for FRN: {frn}")
    try:
        raw_address = client.get_address_by_frn(frn=str(frn), page="1")
    except Exception as e:
        print(f"\n❌  Address lookup failed: {e}")
        sys.exit(1)

    print_json("RAW ADDRESS RESPONSE (Data array)", raw_address)

    norm_address = normalize_address_rows(
        input_company=args.company,
        reference_number=str(frn),
        address_rows=raw_address,
    )
    print_json("NORMALISED ADDRESS ROWS", norm_address)

    print("\n✅  Test complete — compare the raw responses above against Insomnia.")


if __name__ == "__main__":
    main()

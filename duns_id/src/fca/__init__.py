# duns_id/src/fca — FCA Broker Lookup Module
#
# Queries the FCA Broker Query API to enrich client records with
# FCA reference numbers, firm details, and registered addresses.
#
# Typical flow:
#   1. Load input (D_CUSTOMER_NAME or reference_number from enriched DUNS data)
#   2. Search firms via the FCA API
#   3. Retrieve addresses for matched FRNs
#   4. Export search_results.csv, address_results.csv, merged_results.csv, errors.csv

from .client import FCAClient
from .config import FCAConfig

__all__ = ["FCAClient", "FCAConfig"]

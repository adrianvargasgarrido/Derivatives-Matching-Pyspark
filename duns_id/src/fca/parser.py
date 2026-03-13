# =============================================================================
# parser.py — Normalise raw FCA API responses into flat dicts
# =============================================================================
#
# Two normalisers:
#   normalize_search_rows   — from search endpoint
#   normalize_address_rows  — from address endpoint
# =============================================================================

from typing import Any, Dict, List


def join_address_lines(address_row: Dict[str, Any]) -> str:
    """Concatenate Address Line 1–4 into a single comma-separated string."""
    parts = [
        address_row.get("Address Line 1", ""),
        address_row.get("Address Line 2", ""),
        address_row.get("Address Line 3", ""),
        address_row.get("Address Line 4", ""),
    ]
    return ", ".join([str(p).strip() for p in parts if p and str(p).strip()])


def normalize_search_rows(
    input_company: str,
    search_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Flatten search results into a list of dicts ready for a DataFrame."""
    output = []

    for row in search_rows:
        output.append(
            {
                "input_company": input_company,
                "reference_number": row.get("Reference Number"),
                "type_of_business_or_individual": row.get(
                    "Type of business or Individual"
                ),
                "name": row.get("Name"),
                "status": row.get("Status"),
                "source_url": row.get("URL"),
            }
        )

    return output


def normalize_address_rows(
    input_company: str,
    reference_number: str,
    address_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Flatten address results into a list of dicts ready for a DataFrame."""
    output = []

    for row in address_rows:
        output.append(
            {
                "input_company": input_company,
                "reference_number": reference_number,
                "address_type": row.get("Address Type"),
                "town": row.get("Town"),
                "country": row.get("Country"),
                "postcode": row.get("Postcode"),
                "full_address": join_address_lines(row),
                "address_line_1": row.get("Address Line 1"),
                "address_line_2": row.get("Address Line 2"),
                "address_line_3": row.get("Address Line 3"),
                "address_line_4": row.get("Address Line 4"),
                "website_address": row.get("Website Address"),
                "phone_number": row.get("Phone Number"),
                "individual": row.get("Individual"),
                "source_url": row.get("URL"),
            }
        )

    return output

# =============================================================================
# client.py — HTTP client for the FCA Broker Query API
# =============================================================================
#
# Provides two main methods:
#   - search_firm(company_name)    → search by firm name
#   - get_address_by_frn(frn)      → address lookup by FCA Reference Number
#
# Both call the same gateway endpoint with different uriName payloads.
# The gateway wraps the actual FCA response inside a JSON-encoded string
# (response_json["data"]), which we decode transparently.
# =============================================================================

import json
import uuid
from typing import Any, Dict, List

import requests

from .config import FCAConfig


class FCAClient:
    """Stateless client for the FCA Broker Query API gateway."""

    def __init__(self, config: FCAConfig, timeout: int = 30) -> None:
        self.config = config
        self.timeout = timeout

    # ── internal helpers ─────────────────────────────────────────────────────

    def _build_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.bearer_token}",
            "Content-Type": "application/json",
            "channel-id": self.config.channel_id,
            "correlation-id": str(uuid.uuid4()),
        }

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            self.config.base_url,
            headers=self._build_headers(),
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_gateway_response(response_json: Dict[str, Any]) -> Dict[str, Any]:
        """Decode the gateway envelope.

        Gateway shape::

            { "meta": {...}, "data": "{\"Status\":\"...\",\"Data\":[...]}" }

        ``data`` may be a JSON string *or* an already-parsed dict.
        """
        raw_data = response_json.get("data")

        if raw_data is None:
            raise ValueError("Gateway response missing 'data' field")

        if isinstance(raw_data, str):
            return json.loads(raw_data)

        if isinstance(raw_data, dict):
            return raw_data

        raise ValueError(f"Unexpected response_json['data'] type: {type(raw_data)}")

    # ── public API ───────────────────────────────────────────────────────────

    def search_firm(self, company_name: str, page: str = "1") -> List[Dict[str, Any]]:
        """Search the FCA register for a firm by name.

        Returns a list of result dicts, each containing at least:
        ``Reference Number``, ``Name``, ``Status``, ``Type of business or
        Individual``, ``URL``.
        """
        payload = {
            "uriName": "search",
            "uriParameter": {
                "Query": company_name,
                "type": "firm",
                "pagenp": page,
            },
            "authEmail": self.config.auth_email,
            "authkey": self.config.auth_key,
        }

        response_json = self._post(payload)
        parsed = self._parse_gateway_response(response_json)
        return parsed.get("Data", [])

    def get_address_by_frn(self, frn: str, page: str = "1") -> List[Dict[str, Any]]:
        """Retrieve registered address(es) for a given FCA Reference Number.

        Returns a list of address dicts with fields like ``Address Line 1``,
        ``Town``, ``Country``, ``Postcode``, etc.
        """
        payload = {
            "uriName": "Address",
            "uriParameter": {
                "FRN": str(frn),
                "pagenp": page,
            },
            "authEmail": self.config.auth_email,
            "authkey": self.config.auth_key,
        }

        response_json = self._post(payload)
        parsed = self._parse_gateway_response(response_json)
        return parsed.get("Data", [])

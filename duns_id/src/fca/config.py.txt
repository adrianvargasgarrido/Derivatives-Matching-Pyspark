# =============================================================================
# config.py — FCA API configuration loaded from environment variables
# =============================================================================
#
# Reads settings from a .env file (or real env vars) using python-dotenv.
# Place your .env in the duns_id/ project root, e.g.:
#
#   duns_id/.env
#
# See .env.example for the required keys.
# =============================================================================

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Walk up from this file to the duns_id/ root and load .env from there
_PROJECT_ROOT = Path(__file__).resolve().parents[2]  # src/fca/config.py → duns_id/
load_dotenv(_PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class FCAConfig:
    """Immutable configuration for the FCA Broker Query API."""

    base_url: str
    bearer_token: str
    channel_id: str
    auth_email: str
    auth_key: str
    request_delay_seconds: float = 0.5

    @classmethod
    def from_env(cls) -> "FCAConfig":
        """Build config from environment variables.  Raises ValueError if any
        required variable is missing or empty."""

        base_url = os.getenv("FCA_BASE_URL")
        bearer_token = os.getenv("FCA_BEARER_TOKEN")
        channel_id = os.getenv("FCA_CHANNEL_ID")
        auth_email = os.getenv("FCA_AUTH_EMAIL")
        auth_key = os.getenv("FCA_AUTH_KEY")
        request_delay_seconds = float(os.getenv("FCA_REQUEST_DELAY_SECONDS", "0.5"))

        missing = [
            name
            for name, value in {
                "FCA_BASE_URL": base_url,
                "FCA_BEARER_TOKEN": bearer_token,
                "FCA_CHANNEL_ID": channel_id,
                "FCA_AUTH_EMAIL": auth_email,
                "FCA_AUTH_KEY": auth_key,
            }.items()
            if not value
        ]

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                f"Create a .env file at: {_PROJECT_ROOT / '.env'}\n"
                f"See .env.example for the template."
            )

        return cls(
            base_url=base_url,
            bearer_token=bearer_token,
            channel_id=channel_id,
            auth_email=auth_email,
            auth_key=auth_key,
            request_delay_seconds=request_delay_seconds,
        )

"""Economic indicators dashboard - fetches and displays macroeconomic data."""

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from openbb import obb

from config import ECONOMIC_INDICATORS, PIPELINE_DEFAULTS
from database import Database
from retry import retry_fetch

logger = logging.getLogger(__name__)


class EconomicDashboard:
    """Dashboard for economic indicators and macro data."""

    def __init__(self):
        self.db = Database()
        obb.user.preferences.output_type = "dataframe"

    def fetch_fred_series(
        self, series_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """Fetch FRED economic series data.

        Note: Requires FRED API key to be configured.
        """
        try:
            if not start_date:
                start = datetime.now() - timedelta(days=365 * 5)  # 5 years
                start_date = start.strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")

            def _call():
                return obb.economy.fred_series(
                    symbol=series_id, start_date=start_date, end_date=end_date
                )

            data = retry_fetch(_call, description=f"FRED {series_id}")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["series_id"] = series_id
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching FRED series %s: %s", series_id, e)
        return None

    def fetch_gdp_real(self) -> Optional[pd.DataFrame]:
        """Fetch real GDP data (no API key required)."""
        try:
            def _call():
                return obb.economy.gdp.real()

            data = retry_fetch(_call, description="GDP real")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching GDP real: %s", e)
        return None

    def fetch_gdp_nominal(self) -> Optional[pd.DataFrame]:
        """Fetch nominal GDP data (no API key required)."""
        try:
            def _call():
                return obb.economy.gdp.nominal()

            data = retry_fetch(_call, description="GDP nominal")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching GDP nominal: %s", e)
        return None

    def fetch_cpi(self) -> Optional[pd.DataFrame]:
        """Fetch Consumer Price Index data."""
        try:
            def _call():
                return obb.economy.cpi()

            data = retry_fetch(_call, description="CPI")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching CPI: %s", e)
        return None

    def fetch_unemployment(self) -> Optional[pd.DataFrame]:
        """Fetch unemployment rate data."""
        try:
            def _call():
                return obb.economy.unemployment()

            data = retry_fetch(_call, description="unemployment")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching unemployment: %s", e)
        return None

    def fetch_interest_rates(self) -> Optional[pd.DataFrame]:
        """Fetch interest rates data."""
        try:
            def _call():
                return obb.economy.interest_rates()

            data = retry_fetch(_call, description="interest rates")

            if data is not None:
                df = data.to_dataframe() if hasattr(data, "to_dataframe") else data
                df["fetched_at"] = datetime.now().isoformat()
                return df

        except Exception as e:
            logger.error("Error fetching interest rates: %s", e)
        return None

    def update_all_indicators(self):
        """Update all available economic indicators."""
        delay = PIPELINE_DEFAULTS["api_call_delay"]

        logger.info("Updating economic indicators...")

        indicators = [
            ("GDP_REAL", "GDP Real", self.fetch_gdp_real),
            ("GDP_NOMINAL", "GDP Nominal", self.fetch_gdp_nominal),
            ("CPI", "CPI", self.fetch_cpi),
            ("UNEMPLOYMENT", "Unemployment", self.fetch_unemployment),
            ("INTEREST_RATES", "Interest Rates", self.fetch_interest_rates),
        ]

        for series_id, label, fetch_fn in indicators:
            logger.info("  Fetching %s...", label)
            data = fetch_fn()
            if data is not None and not data.empty:
                self.db.save_economic_indicators(data, series_id)
                logger.info("  %s: %d rows saved", label, len(data))
            else:
                logger.warning("  %s: no data returned", label)
            time.sleep(delay)

        # FRED indicators (requires API key)
        logger.info("  FRED indicators (requires API key):")
        for series_id, description in ECONOMIC_INDICATORS.items():
            logger.info("    %s (%s)...", series_id, description)
            data = self.fetch_fred_series(series_id)
            if data is not None and not data.empty:
                self.db.save_economic_indicators(data, series_id)
                logger.info("    %s: %d rows saved", series_id, len(data))
            else:
                logger.warning("    %s: no data (needs API key)", series_id)
            time.sleep(delay)

    def get_economic_summary(self) -> pd.DataFrame:
        """Get summary of latest economic indicators."""
        return self.db.get_latest_economic_indicators()

    def generate_dashboard_report(self) -> str:
        """Generate a text report of economic indicators."""
        report = []
        report.append("# Economic Indicators Dashboard")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # GDP
        gdp = self.fetch_gdp_real()
        if gdp is not None and not gdp.empty:
            latest = gdp.iloc[-1]
            report.append("## GDP (Real)")
            report.append(f"- Latest value: {latest['value']:,.0f}")
            report.append(f"- Date: {latest.name if hasattr(latest, 'name') else 'N/A'}")
            report.append("")

        # CPI
        cpi = self.fetch_cpi()
        if cpi is not None and not cpi.empty:
            latest = cpi.iloc[-1]
            report.append("## Consumer Price Index")
            report.append(f"- Latest value: {latest.get('value', 'N/A')}")
            report.append("")

        # Unemployment
        unemp = self.fetch_unemployment()
        if unemp is not None and not unemp.empty:
            latest = unemp.iloc[-1]
            report.append("## Unemployment Rate")
            report.append(f"- Latest value: {latest.get('value', 'N/A')}")
            report.append("")

        return "\n".join(report)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    dashboard = EconomicDashboard()

    logger.info("=" * 60)
    logger.info("Economic Indicators Dashboard")
    logger.info("=" * 60)

    # Update all indicators
    dashboard.update_all_indicators()

    # Generate report
    logger.info("=" * 60)
    logger.info("Dashboard Report")
    logger.info("=" * 60)
    report = dashboard.generate_dashboard_report()
    print(report)

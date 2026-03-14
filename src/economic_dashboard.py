"""Economic indicators dashboard - fetches and displays macroeconomic data."""

import sys
import sqlite3
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from openbb import obb

from config import ECONOMIC_INDICATORS
from database import Database

logger = logging.getLogger(__name__)


class EconomicDashboard:
    """Dashboard for economic indicators and macro data."""
    
    def __init__(self):
        self.db = Database()
        obb.user.preferences.output_type = "dataframe"
    
    def fetch_fred_series(self, series_id: str, start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch FRED economic series data.
        
        Note: Requires FRED API key to be configured.
        """
        try:
            if not start_date:
                start = datetime.now() - timedelta(days=365*5)  # 5 years
                start_date = start.strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
            
            data = obb.economy.fred_series(
                symbol=series_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['series_id'] = series_id
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching FRED series {series_id}: {e}")
        return None
    
    def fetch_gdp_real(self) -> Optional[pd.DataFrame]:
        """Fetch real GDP data (no API key required)."""
        try:
            data = obb.economy.gdp.real()
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching GDP real: {e}")
        return None
    
    def fetch_gdp_nominal(self) -> Optional[pd.DataFrame]:
        """Fetch nominal GDP data (no API key required)."""
        try:
            data = obb.economy.gdp.nominal()
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching GDP nominal: {e}")
        return None
    
    def fetch_cpi(self) -> Optional[pd.DataFrame]:
        """Fetch Consumer Price Index data."""
        try:
            data = obb.economy.cpi()
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching CPI: {e}")
        return None
    
    def fetch_unemployment(self) -> Optional[pd.DataFrame]:
        """Fetch unemployment rate data."""
        try:
            data = obb.economy.unemployment()
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching unemployment: {e}")
        return None
    
    def fetch_interest_rates(self) -> Optional[pd.DataFrame]:
        """Fetch interest rates data."""
        try:
            data = obb.economy.interest_rates()
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['fetched_at'] = datetime.now().isoformat()
                return df
        
        except Exception as e:
            print(f"Error fetching interest rates: {e}")
        return None
    
    def update_all_indicators(self):
        """Update all available economic indicators."""
        print("Updating economic indicators...")
        
        # GDP Real (no API key)
        print("  Fetching GDP Real...", end=" ")
        gdp_real = self.fetch_gdp_real()
        if gdp_real is not None and not gdp_real.empty:
            self.db.save_economic_indicators(gdp_real, "GDP_REAL")
            print(f"✓ ({len(gdp_real)} rows)")
        else:
            print("✗")
        
        # GDP Nominal (no API key)
        print("  Fetching GDP Nominal...", end=" ")
        gdp_nominal = self.fetch_gdp_nominal()
        if gdp_nominal is not None and not gdp_nominal.empty:
            self.db.save_economic_indicators(gdp_nominal, "GDP_NOMINAL")
            print(f"✓ ({len(gdp_nominal)} rows)")
        else:
            print("✗")
        
        # CPI
        print("  Fetching CPI...", end=" ")
        cpi = self.fetch_cpi()
        if cpi is not None and not cpi.empty:
            self.db.save_economic_indicators(cpi, "CPI")
            print(f"✓ ({len(cpi)} rows)")
        else:
            print("✗")
        
        # Unemployment
        print("  Fetching Unemployment...", end=" ")
        unemp = self.fetch_unemployment()
        if unemp is not None and not unemp.empty:
            self.db.save_economic_indicators(unemp, "UNEMPLOYMENT")
            print(f"✓ ({len(unemp)} rows)")
        else:
            print("✗")
        
        # Interest Rates
        print("  Fetching Interest Rates...", end=" ")
        rates = self.fetch_interest_rates()
        if rates is not None and not rates.empty:
            self.db.save_economic_indicators(rates, "INTEREST_RATES")
            print(f"✓ ({len(rates)} rows)")
        else:
            print("✗")
        
        # FRED indicators (requires API key)
        print("\n  FRED indicators (requires API key):")
        for series_id, description in ECONOMIC_INDICATORS.items():
            print(f"    {series_id} ({description})...", end=" ")
            data = self.fetch_fred_series(series_id)
            if data is not None and not data.empty:
                self.db.save_economic_indicators(data, series_id)
                print(f"✓ ({len(data)} rows)")
            else:
                print("✗ (needs API key)")
    
    def get_economic_summary(self) -> pd.DataFrame:
        """Get summary of latest economic indicators."""
        query = """
            SELECT e1.series_id, e1.date, e1.value
            FROM economic_indicators e1
            WHERE e1.date = (
                SELECT MAX(e2.date) FROM economic_indicators e2
                WHERE e2.series_id = e1.series_id
            )
            ORDER BY e1.series_id
        """
        try:
            with sqlite3.connect(str(self.db.db_path)) as conn:
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error("Error fetching economic summary: %s", e)
            return pd.DataFrame()
    
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
    dashboard = EconomicDashboard()
    
    print("=" * 60)
    print("Economic Indicators Dashboard")
    print("=" * 60)
    print()
    
    # Update all indicators
    dashboard.update_all_indicators()
    print()
    
    # Generate report
    print("=" * 60)
    print("Dashboard Report")
    print("=" * 60)
    report = dashboard.generate_dashboard_report()
    print(report)

"""Tests for portfolio holdings and allocation logic."""

import pytest


@pytest.mark.unit
class TestHoldings:
    """Test holdings CRUD in database."""

    def test_get_holdings_empty(self, tmp_db):
        """Empty database returns empty DataFrame."""
        df = tmp_db.get_holdings()
        assert df.empty
        assert list(df.columns) == ["symbol", "shares"]

    def test_update_and_get_holding(self, tmp_db):
        """Insert a holding and retrieve it."""
        tmp_db.update_holding("AAPL", 100)
        df = tmp_db.get_holdings()
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "AAPL"
        assert df.iloc[0]["shares"] == 100.0

    def test_update_holding_overwrites(self, tmp_db):
        """Updating same symbol replaces shares."""
        tmp_db.update_holding("AAPL", 100)
        tmp_db.update_holding("AAPL", 250)
        df = tmp_db.get_holdings()
        assert len(df) == 1
        assert df.iloc[0]["shares"] == 250.0

    def test_zero_shares_excluded(self, tmp_db):
        """Holdings with 0 shares are not returned."""
        tmp_db.update_holding("AAPL", 100)
        tmp_db.update_holding("MSFT", 0)
        df = tmp_db.get_holdings()
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "AAPL"

    def test_multiple_holdings(self, tmp_db):
        """Multiple symbols stored and retrieved sorted."""
        tmp_db.update_holding("NVDA", 50)
        tmp_db.update_holding("AAPL", 100)
        tmp_db.update_holding("MSFT", 75)
        df = tmp_db.get_holdings()
        assert len(df) == 3
        assert list(df["symbol"]) == ["AAPL", "MSFT", "NVDA"]

    def test_fractional_shares(self, tmp_db):
        """Fractional shares are supported."""
        tmp_db.update_holding("AAPL", 10.5)
        df = tmp_db.get_holdings()
        assert df.iloc[0]["shares"] == 10.5


@pytest.mark.unit
class TestAllocationMath:
    """Test allocation calculations (market value = shares x price)."""

    def test_market_value_single_symbol(self, tmp_db):
        """Market value = shares * price."""
        tmp_db.update_holding("AAPL", 10)
        holdings = tmp_db.get_holdings()
        prices = {"AAPL": 200.0}
        holdings["market_value"] = holdings["symbol"].map(prices) * holdings["shares"]
        assert holdings.iloc[0]["market_value"] == 2000.0

    def test_market_value_multiple_symbols(self, tmp_db):
        """Total portfolio value sums correctly."""
        tmp_db.update_holding("AAPL", 10)
        tmp_db.update_holding("MSFT", 5)
        holdings = tmp_db.get_holdings()
        prices = {"AAPL": 200.0, "MSFT": 400.0}
        holdings["market_value"] = holdings["symbol"].map(prices) * holdings["shares"]
        total = holdings["market_value"].sum()
        assert total == pytest.approx(4000.0)  # 10*200 + 5*400

    def test_sector_aggregation(self, tmp_db):
        """Market values group by sector correctly."""
        tmp_db.update_holding("AAPL", 10)
        tmp_db.update_holding("MSFT", 5)
        tmp_db.update_holding("BABA", 20)
        holdings = tmp_db.get_holdings()
        prices = {"AAPL": 200.0, "MSFT": 400.0, "BABA": 100.0}

        sector_map = {"AAPL": "Tech", "MSFT": "Tech", "BABA": "China"}
        holdings["market_value"] = holdings["symbol"].map(prices) * holdings["shares"]
        holdings["sector"] = holdings["symbol"].map(sector_map)
        grouped = holdings.groupby("sector")["market_value"].sum()

        assert grouped["Tech"] == pytest.approx(4000.0)  # 10*200 + 5*400
        assert grouped["China"] == pytest.approx(2000.0)  # 20*100

    def test_percentage_allocation(self, tmp_db):
        """Percentage allocation sums to 100."""
        tmp_db.update_holding("AAPL", 10)
        tmp_db.update_holding("MSFT", 10)
        holdings = tmp_db.get_holdings()
        prices = {"AAPL": 200.0, "MSFT": 300.0}
        holdings["market_value"] = holdings["symbol"].map(prices) * holdings["shares"]
        total = holdings["market_value"].sum()
        holdings["pct"] = holdings["market_value"] / total * 100
        assert holdings["pct"].sum() == pytest.approx(100.0)
        assert holdings.loc[holdings["symbol"] == "AAPL", "pct"].iloc[0] == pytest.approx(40.0)
        assert holdings.loc[holdings["symbol"] == "MSFT", "pct"].iloc[0] == pytest.approx(60.0)

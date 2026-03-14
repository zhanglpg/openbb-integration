"""
Watchlist Manager - Load and manage symbol watchlists
"""

from pathlib import Path
from typing import List, Set


class WatchlistManager:
    """Manage watchlist of stock symbols"""

    def __init__(self, watchlist_path: str = None):
        """
        Initialize watchlist manager

        Args:
            watchlist_path: Path to watchlist file (one symbol per line)
        """
        if watchlist_path is None:
            watchlist_path = Path(__file__).parent.parent / "data" / "watchlist.txt"
        self.watchlist_path = Path(watchlist_path)
        self.symbols: List[str] = []
        self.load()

    def load(self) -> List[str]:
        """Load symbols from watchlist file"""
        if not self.watchlist_path.exists():
            self.symbols = []
            return self.symbols

        with open(self.watchlist_path, "r") as f:
            self.symbols = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        return self.symbols

    def save(self):
        """Save symbols to watchlist file"""
        self.watchlist_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.watchlist_path, "w") as f:
            f.write("# Watchlist - One symbol per line\n")
            for symbol in self.symbols:
                f.write(f"{symbol}\n")

    def add(self, symbol: str) -> bool:
        """Add symbol to watchlist"""
        symbol = symbol.upper().strip()
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            self.save()
            return True
        return False

    def remove(self, symbol: str) -> bool:
        """Remove symbol from watchlist"""
        symbol = symbol.upper().strip()
        if symbol in self.symbols:
            self.symbols.remove(symbol)
            self.save()
            return True
        return False

    def get_symbols(self) -> List[str]:
        """Get all symbols"""
        return self.symbols.copy()

    def get_symbol_set(self) -> Set[str]:
        """Get symbols as a set for fast lookup"""
        return set(self.symbols)

    def __len__(self):
        return len(self.symbols)

    def __iter__(self):
        return iter(self.symbols)


if __name__ == "__main__":
    # Test
    wl = WatchlistManager()
    print(f"Loaded {len(wl)} symbols: {wl.get_symbols()}")

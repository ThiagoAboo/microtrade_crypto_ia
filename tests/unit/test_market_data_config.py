import pytest
from pydantic import ValidationError

from core.config import MarketDataSettings


def test_market_data_rejects_more_than_five_symbols() -> None:
    with pytest.raises(ValidationError):
        MarketDataSettings(symbols=("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT"))


def test_market_data_parses_comma_separated_symbols() -> None:
    settings = MarketDataSettings(symbols="btcusdt, ethusdt")

    assert settings.symbols == ("BTCUSDT", "ETHUSDT")


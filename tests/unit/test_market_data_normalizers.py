from market_data.normalizers import normalize_depth_update, normalize_trade


def test_normalize_trade_includes_replay_fields() -> None:
    trade = normalize_trade(
        {
            "e": "trade",
            "E": 1672515782136,
            "s": "BNBBTC",
            "t": 12345,
            "p": "0.001",
            "q": "100",
            "T": 1672515782136,
            "m": False,
            "M": True,
        },
        received_ts=1672515782140,
    )

    assert trade.symbol == "BNBBTC"
    assert trade.source_exchange == "binance_spot"
    assert trade.sequence == 12345
    assert trade.side == "buy"
    assert trade.exchange_ts == 1672515782136
    assert trade.received_ts == 1672515782140
    assert trade.processed_ts >= trade.received_ts


def test_normalize_depth_update_maps_u_fields() -> None:
    update = normalize_depth_update(
        {
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BNBBTC",
            "U": 157,
            "u": 160,
            "b": [["0.0024", "10"]],
            "a": [["0.0026", "100"]],
        },
        received_ts=1672515782140,
    )

    assert update.first_update_id == 157
    assert update.final_update_id == 160
    assert update.bids == (("0.0024", "10"),)
    assert update.asks == (("0.0026", "100"),)


"""Reference-driven port resolution."""
from __future__ import annotations

from maritimeiq.utils.ports import PortResolver, clean_port_text


def _resolver() -> PortResolver:
    names = {"SHANGHAI": "CNSHA", "YANGSHAN": "CNSHA", "LOS ANGELES": "USLAX", "BUSAN": "KRPUS", "JEBEL ALI": "AEJEA"}
    return PortResolver(
        locodes={"CNSHA", "USLAX", "USLGB", "KRPUS", "AEJEA"},
        names=names,
        location_codes={"SHA": "CNSHA", "LGB": "USLGB"},
        human_aliases={"SHENZHEN": "CNYTN"},
    )


def test_clean_text() -> None:
    assert clean_port_text("KRPUS-PUSC") == "KRPUS"
    assert clean_port_text("Port of Shanghai") == "SHANGHAI"
    assert clean_port_text("Jebel Ali, AE") == "JEBEL ALI"


def test_resolution_methods() -> None:
    r = _resolver()
    assert r.resolve("CN SHA")[0] == "CNSHA"
    assert r.resolve("Yangshan")[0] == "CNSHA"
    assert r.resolve("LGB")[0] == "USLGB"
    assert r.resolve("POLA")[0] == "USLAX"
    assert r.resolve("LOS ANGELES CA")[0] == "USLAX"
    assert r.resolve("BUSAN NEW PORT")[0] == "KRPUS"
    assert r.resolve("shenzhen") == ("CNYTN", "human")


def test_unresolved_goes_to_hitl() -> None:
    assert _resolver().resolve("Pasir Panjang") == (None, "unresolved")
    assert _resolver().resolve("FOR ORDERS") == (None, "empty")

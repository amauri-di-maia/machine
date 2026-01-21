from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import re
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class RawSearchItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    qty: int
    condition: str
    category: Optional[str] = None
    price: Optional[str] = None


def _get_text(parent: ET.Element, tag: str) -> Optional[str]:
    el = parent.find(tag)
    if el is None or el.text is None:
        return None
    s = el.text.strip()
    return s if s != "" else None


def parse_search_xml(path: str) -> List[RawSearchItem]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"search.xml not found: {path}")

    text = p.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        raise ValueError(f"search.xml is empty: {path}")

    # Muitos ficheiros vêm sem root; embrulhamos para garantir parse.
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        wrapped = f"<ROOT>\n{text}\n</ROOT>"
        root = ET.fromstring(wrapped)

    out: List[RawSearchItem] = []
    for item in root.findall(".//ITEM"):
        bl_part_id = _get_text(item, "ITEMID")
        bl_itemtype = (_get_text(item, "ITEMTYPE") or "P").strip().upper()
        bl_color_raw = _get_text(item, "COLOR")
        qty_raw = _get_text(item, "QTY")
        condition = (_get_text(item, "CONDITION") or "N").strip().upper()

        if not bl_part_id:
            raise ValueError("Found ITEM with missing ITEMID in search.xml")

        try:
            qty = int(float(qty_raw)) if qty_raw is not None else 0
        except ValueError:
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={bl_part_id}")

        if qty <= 0:
            # Ignorar itens com qty 0 (não faz sentido otimizar)
            continue

        bl_color_id: Optional[int] = None
        if bl_color_raw is not None:
            try:
                bl_color_id = int(bl_color_raw)
            except ValueError:
                raise ValueError(f"Invalid COLOR='{bl_color_raw}' for ITEMID={bl_part_id}")

        if condition not in {"N", "U"}:
            raise ValueError(f"Invalid CONDITION='{condition}' for ITEMID={bl_part_id} (expected N or U)")

        out.append(
            RawSearchItem(
                bl_itemtype=bl_itemtype,
                bl_part_id=bl_part_id,
                bl_color_id=bl_color_id,
                qty=qty,
                condition=condition,
                category=_get_text(item, "CATEGORY"),
                price=_get_text(item, "PRICE"),
            )
        )

    return out

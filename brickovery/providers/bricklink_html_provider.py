from __future__ import annotations

import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Dict, Tuple, List, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


EU_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
    "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE"
}
EXCLUDE_ISO2 = {"GB", "CH"}  # UK, Switzerland

PRICE_RE = re.compile(r"(?P<tilde>~)?\s*(?P<cur>EUR)\s*(?P<num>\d[\d\.,]*)", re.IGNORECASE)
BOX_RE = re.compile(r"box16([YN])\.png", re.IGNORECASE)
FLAG_RE = re.compile(r"/flagsS/([A-Z]{2})\.(gif|png)", re.IGNORECASE)
STORE_RE = re.compile(r"store\.asp\?", re.IGNORECASE)

# Resolver idItem no HTML "catalogitem.page"
IDITEM_RE = re.compile(r"[?&]idItem=(\d+)", re.IGNORECASE)
IDITEM_JSON_RE = re.compile(r'"idItem"\s*:\s*(\d+)', re.IGNORECASE)


@dataclass(frozen=True)
class Offer:
    country_iso2: Optional[str]
    ships_to_me: Optional[bool]
    qty: int
    price: Optional[Decimal]
    is_estimate: bool
    store_url: Optional[str]


@dataclass(frozen=True)
class Sold6M:
    total_qty: Optional[int]
    avg_price: Optional[Decimal]


@dataclass(frozen=True)
class CurrentStats:
    total_lots: int
    total_qty: int
    min_price: Optional[Decimal]
    min_price_is_estimate: Optional[bool]


def parse_price_cell(text: str) -> Tuple[Optional[Decimal], bool]:
    text = " ".join(text.split())
    m = PRICE_RE.search(text)
    if not m:
        return None, False
    is_est = bool(m.group("tilde"))
    num = m.group("num")

    if "," in num and "." in num:
        num = num.replace(",", "")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")

    return Decimal(num), is_est


def parse_offers_from_html(html: str) -> List[Offer]:
    soup = BeautifulSoup(html, "html.parser")
    offers: List[Offer] = []

    for tr in soup.find_all("tr"):
        a = tr.find("a", href=STORE_RE)
        if not a:
            continue

        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        country_iso2 = None
        flag_img = tr.find("img", src=FLAG_RE)
        if flag_img and flag_img.get("src"):
            m = FLAG_RE.search(flag_img["src"])
            if m:
                country_iso2 = m.group(1).upper()

        ships_to_me = None
        box_img = tr.find("img", src=BOX_RE)
        if box_img and box_img.get("src"):
            m = BOX_RE.search(box_img["src"])
            if m:
                ships_to_me = (m.group(1).upper() == "Y")

        qty_txt = tds[1].get_text(strip=True)
        qty = int(qty_txt) if qty_txt.isdigit() else 0

        price = None
        is_est = False
        for td in (tds[-1], *tds[:-1]):
            p, est = parse_price_cell(td.get_text(" ", strip=True))
            if p is not None:
                price, is_est = p, est
                break

        store_url = None
        href = a.get("href")
        if href:
            store_url = href.replace("http://", "https://")
            store_url = urljoin("https://www.bricklink.com/", store_url)

        offers.append(Offer(
            country_iso2=country_iso2,
            ships_to_me=ships_to_me,
            qty=qty,
            price=price,
            is_estimate=is_est,
            store_url=store_url
        ))

    return offers


def filter_offers_eu_ships_strict(offers: List[Offer]) -> List[Offer]:
    """
    Regra do projeto (V1): filtrar UE-only + ships-to-PT ANTES de métricas.
    Aqui usamos box16Y como "ships_to_me==True" (exige cookie/sessão configurada).
    """
    out = []
    for o in offers:
        if o.ships_to_me is not True:
            continue
        if not o.country_iso2:
            continue
        c = o.country_iso2.upper()
        if c in EXCLUDE_ISO2:
            continue
        if c not in EU_ISO2:
            continue
        out.append(o)
    return out


def compute_current_stats(offers: List[Offer]) -> CurrentStats:
    total_lots = len(offers)
    total_qty = sum(o.qty for o in offers)

    min_price = None
    min_is_est = None
    for o in offers:
        if o.price is None:
            continue
        if (min_price is None) or (o.price < min_price):
            min_price = o.price
            min_is_est = o.is_estimate

    return CurrentStats(
        total_lots=total_lots,
        total_qty=total_qty,
        min_price=min_price,
        min_price_is_estimate=min_is_est
    )


def parse_price_guide_summary_sold6m(html: str) -> Dict[str, Sold6M]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [" ".join(l.split()) for l in text.splitlines() if l.strip()]

    in_sold = False
    out: Dict[str, Sold6M] = {"new": Sold6M(None, None), "used": Sold6M(None, None)}

    for line in lines:
        if "Past 6 Months Sales" in line:
            in_sold = True
            continue
        if in_sold and "Current Items for Sale" in line:
            break

        m = re.match(r"^(New|Used):\s*(.+)$", line, flags=re.IGNORECASE)
        if not (in_sold and m):
            continue

        kind = "new" if m.group(1).lower() == "new" else "used"
        payload = m.group(2)

        nums = re.findall(r"\d[\d,]*(?:\.\d+)?", payload)
        if len(nums) >= 4:
            total_qty = int(nums[1].replace(",", ""))
            avg = nums[3]
            if "," in avg and "." in avg:
                avg = avg.replace(",", "")
            elif "," in avg and "." not in avg:
                avg = avg.replace(",", ".")
            out[kind] = Sold6M(total_qty=total_qty, avg_price=Decimal(avg))

    return out


class BrickLinkHtmlProvider:
    """
    Provider M2 (scrape) – baseado no teu scraper :contentReference[oaicite:2]{index=2}.
    Requer cookie para box16Y refletir PT (ships_to_me).
    """
    def __init__(
        self,
        *,
        cookie: Optional[str],
        delay_s: float = 0.9,
        timeout_s: int = 25,
        max_tries: int = 5,
    ):
        self.cookie = cookie
        self.delay_s = delay_s
        self.timeout_s = timeout_s
        self.max_tries = max_tries
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.bricklink.com/",
        }
        if cookie:
            self.headers["Cookie"] = cookie

        self.offers_url = "https://www.bricklink.com/v2/catalog/catalogitem_pgtab.page"
        self.sold_summary_url = "https://www.bricklink.com/priceGuideSummary.asp"
        self.catalog_item_url = "https://www.bricklink.com/v2/catalog/catalogitem.page"

    def _fetch_html(self, url: str, params: dict, delay_s: float) -> str:
        # delay + backoff simples para 429/5xx
        if delay_s > 0:
            time.sleep(delay_s)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_tries + 1):
            try:
                r = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout_s)
                if r.status_code in (429, 500, 502, 503, 504):
                    # backoff exponencial
                    time.sleep(min(12.0, (2.0 ** attempt)))
                    continue
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                time.sleep(min(12.0, (2.0 ** attempt)))
        raise RuntimeError(f"Failed to fetch {url} after {self.max_tries} tries") from last_err

    def resolve_id_item(self, *, itemtype: str, part_id: str) -> int:
        # Tenta obter idItem (interno) a partir de catalogitem.page
        # Ex.: /v2/catalog/catalogitem.page?P=3005
        params = {itemtype.upper(): part_id}
        html = self._fetch_html(self.catalog_item_url, params=params, delay_s=0.0)

        m = IDITEM_RE.search(html)
        if m:
            return int(m.group(1))

        m = IDITEM_JSON_RE.search(html)
        if m:
            return int(m.group(1))

        # fallback: procurar qualquer "idItem=####" no HTML
        allm = IDITEM_RE.findall(html)
        if allm:
            return int(allm[0])

        raise RuntimeError(f"Could not resolve BrickLink idItem for {itemtype}:{part_id}")

    def fetch_sold_6m(self, *, itemtype: str, id_item: int, id_color: int) -> Dict[str, Sold6M]:
        html = self._fetch_html(
            self.sold_summary_url,
            params={
                "a": itemtype.upper(),
                "itemID": str(id_item),
                "colorID": str(id_color),
                "vatInc": "Y",
                "vcID": "2",  # EUR
            },
            delay_s=0.0,
        )
        return parse_price_guide_summary_sold6m(html)

    def fetch_current_offers(self, *, id_item: int, id_color: int, st: int) -> Tuple[List[Offer], CurrentStats]:
        html = self._fetch_html(
            self.offers_url,
            params={
                "idItem": str(id_item),
                "idColor": str(id_color),
                "st": str(st),          # 1 new, 2 used
                "gm": "1", "gc": "0", "ei": "0",
                "prec": "3",
                "showflag": "1",
                "showbulk": "1",
                "currency": "2",        # EUR
            },
            delay_s=self.delay_s,
        )
        offers = parse_offers_from_html(html)
        offers = filter_offers_eu_ships_strict(offers)
        stats = compute_current_stats(offers)
        return offers, stats

    def get_item_snapshot(self, *, itemtype: str, part_id: str, color_id: int) -> Dict[str, Any]:
        if not self.cookie:
            raise RuntimeError(
                "BRICKLINK_COOKIE is required for strict ships-to-PT filtering (box16Y). "
                "Set secret BRICKLINK_COOKIE with a valid logged-in cookie."
            )

        id_item = self.resolve_id_item(itemtype=itemtype, part_id=part_id)

        sold = self.fetch_sold_6m(itemtype=itemtype, id_item=id_item, id_color=color_id)

        offers_new, stats_new = self.fetch_current_offers(id_item=id_item, id_color=color_id, st=1)
        offers_used, stats_used = self.fetch_current_offers(id_item=id_item, id_color=color_id, st=2)

        return {
            "id_item": id_item,
            "new": {
                "sold_6m_total_qty": sold["new"].total_qty,
                "sold_6m_avg_price": sold["new"].avg_price,
                "offers": offers_new,
                "stats": stats_new,
            },
            "used": {
                "sold_6m_total_qty": sold["used"].total_qty,
                "sold_6m_avg_price": sold["used"].avg_price,
                "offers": offers_used,
                "stats": stats_used,
            },
        }

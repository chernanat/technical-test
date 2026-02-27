import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional
from helpers import extract_listing_id, load_checkpoint, save_checkpoint, load_urls 
import anthropic
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

INPUT_FILE = "listings.txt"
OUTPUT_CSV = "output/listings_output.csv"
OUTPUT_JSON = "output/listings_output.json"
CHECKPOINT_FILE = "output/checkpoint.json"

CONCURRENCY = 5
REQUEST_DELAY = (2, 3)
PAGE_TIMEOUT = 30_000
MAX_RETRIES = 2

AI_ENABLED = bool(os.getenv("ANTHROPIC_API_KEY"))
AI_MODEL = "claude-haiku-4-5-20251001"

Path("output").mkdir(exist_ok=True)
try:
    sys.stdout.reconfigure(encoding="utf-8")
except AttributeError:
    pass
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("output/etl.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ── Modelos de datos

@dataclass
class ListingResult:
    url: str
    listing_id: str
    status: str = "pending"
    rating: Optional[float] = None
    review_count: Optional[int] = None
    title: Optional[str] = None
    last_5_reviews: list = field(default_factory=list)
    highlight: Optional[str] = None
    opportunity: Optional[str] = None
    error_message: Optional[str] = None
    scraped_at: Optional[str] = None

# ── Scraping 

async def scrape_listing(page, url: str) -> ListingResult:
    listing_id = extract_listing_id(url)
    result = ListingResult(url=url, listing_id=listing_id)

    try:
        await page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

        log.info(f"Obteniendo {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        await page.wait_for_timeout(random.randint(2000, 4000))

        body_text = await page.inner_text("body")
        if any(kw in body_text.lower() for kw in ["captcha", "robot", "access denied", "unusual traffic"]):
            result.status = "blocked"
            result.error_message = "Bot detection triggered"
            log.warning(f"Blocked on {url}")
            return result

        # ── Rating
        rating = None
        try:
            json_ld_elements = await page.query_selector_all('script[type="application/ld+json"]')
            for el in json_ld_elements:
                raw = await el.inner_text()
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if "aggregateRating" in item:
                        rating = float(item["aggregateRating"].get("ratingValue", 0))
                        break
                if rating:
                    break
        except Exception:
            pass

        if not rating:
            for selector in [
                '[data-testid="pdp-reviews-highlight-banner-host-rating"] span',
                'span[aria-label*="out of 5"]',
                '._17p6nbba',
                'span.r1lutz1s',
            ]:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        text = await el.inner_text()
                        m = re.search(r"(\d+\.\d+)", text)
                        if m:
                            rating = float(m.group(1))
                            break
                except Exception:
                    continue

        result.rating = rating

        # ── Review Count
        review_count = None
        try:
            json_ld_elements = await page.query_selector_all('script[type="application/ld+json"]')
            for el in json_ld_elements:
                raw = await el.inner_text()
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if "aggregateRating" in item:
                        review_count = int(item["aggregateRating"].get("reviewCount", 0))
                        break
                if review_count is not None:
                    break
        except Exception:
            pass

        if not review_count:
            for selector in [
                'a[href*="reviews"] span',
                '[data-testid="pdp-reviews-highlight-banner-host-rating"] button',
                'button[data-testid*="review"]',
                '._s65ijh7',
            ]:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        text = await el.inner_text()
                        m = re.search(r"(\d[\d,]*)", text)
                        if m:
                            review_count = int(m.group(1).replace(",", ""))
                            break
                except Exception:
                    continue

        result.review_count = review_count

        # ── Title
        try:
            el = await page.query_selector("h1")
            if el:
                result.title = (await el.inner_text()).strip()
        except Exception:
            pass

        # ── Reviews (3 capas de fallback)
        reviews = []
        try:
            review_btn = None
            for btn_selector in [
                'button[data-testid="pdp-show-all-reviews-button"]',
                'a[data-testid="pdp-show-all-reviews-button"]',
                'button:has-text("Show all")',
                'button:has-text("reviews")',
            ]:
                try:
                    review_btn = await page.query_selector(btn_selector)
                    if review_btn:
                        break
                except Exception:
                    continue

            if review_btn:
                try:
                    # Cerrar modal de traducción si está abierto (tapa el botón de reseñas)
                    for close_sel in [
                        'button[aria-label="Close"]',
                        'button[aria-label="Cerrar"]',
                        'div[role="dialog"] button:has-text("Got it")',
                        'div[role="dialog"] button:has-text("OK")',
                        'div[role="dialog"] button:has-text("Close")',
                    ]:
                        try:
                            close_btn = await page.query_selector(close_sel)
                            if close_btn:
                                await close_btn.click()
                                await page.wait_for_timeout(500)
                                break
                        except Exception:
                            continue

                    # Scroll hasta el botón y click via dispatchEvent para evitar
                    # el error "element is outside of the viewport"
                    await review_btn.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)
                    await review_btn.dispatch_event("click")
                    await page.wait_for_timeout(3000)
                except Exception:
                    pass

            # Palabras que indican que el texto es un rating, no una reseña
            EXCLUDE_PREFIXES = (
                "rated ", "overall rating", "accuracy", "check-in",
                "cleanliness", "communication", "location", "value",
            )

            text_selectors = [
                # Selector confirmado funcionando via debug_modal.py
                'div[role="dialog"] span',
                'div[role="dialog"] div > span',
                '[data-testid="review-card"] span[style*="-webkit-line-clamp"]',
                '[data-testid="review-card"] span',
                'div[role="dialog"] li span[style]',
                'div[role="dialog"] li span',
                'section[data-testid*="review"] span[style*="-webkit-line-clamp"]',
                'li[class*="review"] span',
                '._1gjypya',
                '.r1bctolv span',
                '[data-section-id="REVIEWS"] li span',
                '[data-section-id="REVIEWS"] p',
            ]

            for sel in text_selectors:
                if reviews:
                    break
                try:
                    els = await page.query_selector_all(sel)
                    seen = set()
                    for el in els:
                        try:
                            text = (await el.inner_text()).strip()
                            # Normalizar: colapsar saltos de linea en espacio
                            text_clean = " ".join(text.split())
                            # Filtros: longitud real de reseña y no es texto de rating
                            if (text_clean
                                    and 40 < len(text_clean) < 2000
                                    and text_clean not in seen
                                    and not text_clean.lower().startswith(EXCLUDE_PREFIXES)):
                                seen.add(text_clean)
                                reviews.append(text_clean)
                            if len(reviews) >= 5:
                                break
                        except Exception:
                            continue
                except Exception:
                    continue

            if not reviews:
                try:
                    next_data_el = await page.query_selector('#__NEXT_DATA__')
                    if next_data_el:
                        raw_json = await next_data_el.inner_text()
                        comment_matches = re.findall(
                            r'"comments"\s*:\s*"((?:[^"\\]|\\.){30,500})"',
                            raw_json
                        )
                        for c in comment_matches[:5]:
                            try:
                                reviews.append(bytes(c, "utf-8").decode("unicode_escape"))
                            except Exception:
                                reviews.append(c)

                        if not reviews:
                            body_matches = re.findall(
                                r'"reviewBody"\s*:\s*"((?:[^"\\]|\\.){30,500})"',
                                raw_json
                            )
                            for b in body_matches[:5]:
                                try:
                                    reviews.append(bytes(b, "utf-8").decode("unicode_escape"))
                                except Exception:
                                    reviews.append(b)
                except Exception:
                    pass

        except Exception as e:
            log.debug(f"Review extraction failed for {url}: {e}")

        result.last_5_reviews = reviews[:5]

        result.status = "success" if (rating is not None or review_count is not None) else "no_data"
        result.scraped_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        log.info(f"[OK] {listing_id} - rating={rating}, reviews={review_count}, texts={len(reviews)}")

    except PWTimeout:
        result.status = "error"
        result.error_message = "Page timeout"
        log.warning(f"Timeout: {url}")
    except Exception as e:
        result.status = "error"
        result.error_message = str(e)[:200]
        log.error(f"Error scraping {url}: {e}")

    return result


# ── AI Insights

def generate_ai_insights(result: ListingResult) -> ListingResult:
    if not AI_ENABLED or not result.last_5_reviews:
        return result

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    reviews_text = "\n\n".join(
        f"Review {i+1}: {r}" for i, r in enumerate(result.last_5_reviews)
    )

    prompt = f"""You are analyzing guest reviews for an Airbnb listing.

Reviews:
{reviews_text}

Based solely on these reviews, provide:
1. HIGHLIGHT: One specific characteristic that guests love most (be concrete, e.g. "Stunning ocean views from the rooftop terrace" not "great location").
2. OPPORTUNITY: One specific improvement guests mention (or "None mentioned" if reviews are uniformly positive).

Respond in this exact JSON format with no other text:
{{"highlight": "...", "opportunity": "..."}}"""

    try:
        response = client.messages.create(
            model=AI_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```json\s*|```$", "", raw, flags=re.MULTILINE).strip()
        insights = json.loads(raw)
        result.highlight = insights.get("highlight")
        result.opportunity = insights.get("opportunity")
        log.info(f"AI insights generados para {result.listing_id}")
    except Exception as e:
        log.warning(f"AI insight failed for {result.listing_id}: {e}")

    return result


# ── Salida de datos

CSV_FIELDS = [
    "listing_id", "url", "status", "title", "rating", "review_count",
    "highlight", "opportunity", "error_message", "scraped_at",
]


def save_outputs(results: list[ListingResult]):
    Path("output").mkdir(exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in results:
            row = asdict(r)
            writer.writerow({k: row.get(k) for k in CSV_FIELDS})

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, ensure_ascii=False)

    log.info(f"Guardados {len(results)} registros en {OUTPUT_CSV} y {OUTPUT_JSON}")


# ── Orquestación principal

async def process_batch(urls: list[str], checkpoint: dict) -> list[ListingResult]:
    results = []
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        async def handle_url(url: str):
            listing_id = extract_listing_id(url)

            if listing_id in checkpoint and checkpoint[listing_id]["status"] in ("success", "blocked"):
                log.info(f"Saltando (checkpoint): {listing_id}")
                r = ListingResult(**checkpoint[listing_id])
                results.append(r)
                return

            async with semaphore:
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                )
                page = await context.new_page()

                await page.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )

                result = None
                for attempt in range(1, MAX_RETRIES + 1):
                    result = await scrape_listing(page, url)
                    if result.status != "error":
                        break
                    if attempt < MAX_RETRIES:
                        wait = random.uniform(5, 10)
                        log.info(f"Reintento {attempt} para {url} en {wait:.1f}s")
                        await asyncio.sleep(wait)

                await context.close()

                if AI_ENABLED and result.last_5_reviews:
                    result = generate_ai_insights(result)

                checkpoint[listing_id] = asdict(result)
                save_checkpoint(checkpoint)
                results.append(result)

                await asyncio.sleep(random.uniform(*REQUEST_DELAY))

        await asyncio.gather(*[handle_url(u) for u in urls])
        await browser.close()

    return results


def print_summary(results: list[ListingResult]):
    total = len(results)
    success = sum(1 for r in results if r.status == "success")
    no_data = sum(1 for r in results if r.status == "no_data")
    errors = sum(1 for r in results if r.status == "error")
    blocked = sum(1 for r in results if r.status == "blocked")
    with_reviews = sum(1 for r in results if r.last_5_reviews)
    with_ai = sum(1 for r in results if r.highlight)

    print("\n" + "=" * 50)
    print("  Resumen del proceso:")
    print("=" * 50)
    print(f"  Total URLs procesadas : {total}")
    print(f"  Success (Encontrada)  : {success}")
    print(f"  Success (Sin data)    : {no_data}")
    print(f"  Errors                : {errors}")
    print(f"  Blocked               : {blocked}")
    print(f"  Con texto de reseñas  : {with_reviews}")
    print(f"  Con AI insights       : {with_ai}")
    print("=" * 50)
    print(f"  Output CSV  : {OUTPUT_CSV}")
    print(f"  Output JSON : {OUTPUT_JSON}")
    print("=" * 50 + "\n")


async def main():
    Path("output").mkdir(exist_ok=True)
    urls = load_urls(INPUT_FILE)
    log.info(f"Cargadas {len(urls)} URLs desde {INPUT_FILE}")

    checkpoint = load_checkpoint()
    log.info(f"Checkpoint: {len(checkpoint)} URLs previamente procesadas")

    results = await process_batch(urls, checkpoint)
    save_outputs(results)
    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())
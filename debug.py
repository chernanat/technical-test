import asyncio
from playwright.async_api import async_playwright

TEST_URL = "https://www.airbnb.com/rooms/23680573"

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = await context.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        print(f"Abriendo {TEST_URL}...")
        await page.goto(TEST_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        # Abrir el modal
        btn = await page.query_selector('button[data-testid="pdp-show-all-reviews-button"]')
        if btn:
            await btn.scroll_into_view_if_needed()
            await btn.click()
            print("Modal abierto, esperando que cargue...")
            await page.wait_for_timeout(3000)

            # Volcar HTML del modal para ver la estructura real
            dialog = await page.query_selector('div[role="dialog"]')
            if dialog:
                html = await dialog.inner_html()
                # Guardar en archivo para revisar
                with open("modal_html.html", "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"HTML del modal guardado en modal_html.html ({len(html)} caracteres)")

                # Intentar todos los spans y ver cuáles tienen texto de reseña
                print("\n--- Probando selectores dentro del dialog ---")
                selectors_to_try = [
                    "span",
                    "p",
                    "div > span",
                    "li span",
                    "li p",
                    "[class] span",
                    "span[class]",
                ]
                for sel in selectors_to_try:
                    els = await dialog.query_selector_all(sel)
                    candidates = []
                    for el in els:
                        try:
                            text = (await el.inner_text()).strip()
                            if 40 < len(text) < 1500 and "\n" not in text[:30]:
                                candidates.append(text)
                        except Exception:
                            continue
                    if candidates:
                        print(f"\n  ✓ '{sel}' encontró {len(candidates)} textos válidos")
                        for c in candidates[:2]:
                            print(f"    → {c[:120]}")
                    else:
                        print(f"  ✗ '{sel}': sin resultados")

            else:
                print("No se encontró div[role='dialog'] después de hacer click")
        else:
            print("No se encontró el botón de reseñas")

        print("\nCerrando en 5 segundos...")
        await page.wait_for_timeout(5000)
        await browser.close()

asyncio.run(debug())
"""
Step 1: Open a real browser, let user log in to Facebook manually,
then save the session (cookies + localStorage) to fb_session.json.

Run: python crawlers/save_fb_session.py
After logging in, press Enter in terminal to save and exit.
"""

import asyncio
import json
import os
from pathlib import Path
from playwright.async_api import async_playwright

SESSION_FILE = Path(__file__).parent / "fb_session.json"


async def main():
    print("=" * 60)
    print("Facebook Session Saver")
    print("=" * 60)
    print()
    print("1. Trình duyệt sẽ mở ra")
    print("2. Đăng nhập Facebook bằng tay")
    print("3. Sau khi login xong, quay lại terminal và nhấn Enter")
    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--start-maximized"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="vi-VN",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        page = await context.new_page()
        await page.goto("https://www.facebook.com/login")

        print(">>> Trình duyệt đã mở. Hãy đăng nhập Facebook...")
        print(">>> Nhấn Enter khi đã login xong: ", end="", flush=True)

        # Wait for user to login
        await asyncio.get_event_loop().run_in_executor(None, input)

        # Check if actually logged in
        current_url = page.url
        if "login" in current_url or "checkpoint" in current_url:
            print("⚠️  Có vẻ chưa login xong. Lưu session anyway...")
        else:
            print(f"✅ Đang ở: {current_url}")

        # Save storage state (cookies + localStorage)
        await context.storage_state(path=str(SESSION_FILE))
        print(f"\n✅ Session đã lưu tại: {SESSION_FILE}")
        print("   Chạy tiếp: python crawlers/fb_group_scraper.py")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

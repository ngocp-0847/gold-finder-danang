"""
Start Chrome with Profile 3 (bombaytera123@gmail.com) + remote debugging on port 9222.
Run this once before running fb_group_scraper.py.

Usage: python crawlers/start_chrome_debug.py
"""

import subprocess, shutil, tempfile, time, sys, os
from pathlib import Path
import urllib.request

CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CHROME_SRC = Path.home() / "Library/Application Support/Google/Chrome"
CDP_PORT = 9222


def check_cdp_ready(port=CDP_PORT, retries=10) -> bool:
    for i in range(retries):
        try:
            urllib.request.urlopen(f"http://localhost:{port}/json/version", timeout=2)
            return True
        except Exception:
            time.sleep(1)
    return False


def main():
    # Kill any existing Chrome
    subprocess.run(["pkill", "-f", "Google Chrome"], capture_output=True)
    time.sleep(1)

    # Create temp user-data-dir with Profile 3 as Default
    tmpdir = Path(tempfile.mkdtemp(prefix="chrome_cdp_"))
    print(f"Temp profile dir: {tmpdir}")

    src_profile = CHROME_SRC / "Profile 3"
    print(f"Copying Profile 3 from: {src_profile}")
    shutil.copytree(str(src_profile), str(tmpdir / "Default"))
    # Copy Local State
    local_state = CHROME_SRC / "Local State"
    if local_state.exists():
        shutil.copy2(str(local_state), str(tmpdir / "Local State"))

    cmd = [
        CHROME_BIN,
        f"--user-data-dir={tmpdir}",
        f"--remote-debugging-port={CDP_PORT}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
        "--window-size=1366,900",
        "https://www.facebook.com/",
    ]

    print(f"Starting Chrome on CDP port {CDP_PORT}...")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Chrome PID: {proc.pid}")

    if check_cdp_ready():
        print(f"✅ Chrome ready! CDP: http://localhost:{CDP_PORT}")
        print(f"\nNow run: python crawlers/fb_group_scraper.py")
        print(f"\nPress Ctrl+C to stop Chrome when done.")
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
            shutil.rmtree(str(tmpdir), ignore_errors=True)
            print("Chrome stopped.")
    else:
        print("❌ Chrome CDP not ready after 10s")
        proc.terminate()
        sys.exit(1)


if __name__ == "__main__":
    main()

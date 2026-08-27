import asyncio
import json
import re
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False
    ProxyConnector = None

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DEAD_FILE = DATA_DIR / "dead_proxies.json"
LIVE_FILE = DATA_DIR / "live_proxies.json"
COUNTRY_DIR = ROOT / "country"

ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
MAX_PROXIES = 50000
SKIP_FIRST = 100000
CONCURRENCY = 100
TIMEOUT = 10
PROTO = "socks5"
PROXRIPPER_URL = "https://raw.githubusercontent.com/Mohammedcha/ProxRipper/refs/heads/main/full_proxies/socks5.txt"

DATA_DIR.mkdir(parents=True, exist_ok=True)
COUNTRY_DIR.mkdir(parents=True, exist_ok=True)


def load_dead_set():
    if DEAD_FILE.exists():
        try:
            data = json.loads(DEAD_FILE.read_text(encoding="utf-8"))
            return set(data.get("dead", []))
        except Exception:
            return set()
    return set()


def save_dead_set(dead_set):
    save_data = {"dead": sorted(dead_set), "updated": time.time(), "count": len(dead_set)}
    DEAD_FILE.write_text(json.dumps(save_data, indent=2), encoding="utf-8")


async def test_proxy(proxy, semaphore):
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            # Use socks connector if available for socks4/5
            if PROTO in ("socks4", "socks5") and HAS_SOCKS:
                connector = ProxyConnector.from_url(f"{PROTO}://{proxy}")
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
            else:
                scheme = PROTO if PROTO in ("socks4", "socks5") else "http"
                # fallback to http scheme if socks lib not available (will fail for socks but not crash)
                proxy_url = f"{scheme}://{proxy}" if PROTO.startswith("socks") else f"http://{proxy}"
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", proxy=proxy_url, timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
        except Exception:
            pass
    return False


async def geolocate_batch(ips):
    result = {}
    uniq_ips = list(dict.fromkeys(ips))
    batches = [uniq_ips[i:i+100] for i in range(0, len(uniq_ips), 100)]
    async with aiohttp.ClientSession() as s:
        for batch in batches:
            payload = [{"query": ip} for ip in batch]
            for attempt in range(3):
                try:
                    async with s.post(
                        "http://ip-api.com/batch",
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for entry in data:
                                if isinstance(entry, dict) and entry.get("status") == "success":
                                    q = entry.get("query", "")
                                    cc = entry.get("countryCode", "").upper()
                                    if cc:
                                        result[q] = cc
                            break
                except Exception:
                    pass
                await asyncio.sleep(1)
            await asyncio.sleep(0.5)
    return result


async def main():
    print(f"[Boost-{PROTO} 100000] Starting ProxRipper {PROTO.upper()} booster THIRD 50k...")
    print(f"[Boost-{PROTO}] Config: SKIP={SKIP_FIRST}, MAX={MAX_PROXIES}, CONCURRENCY={CONCURRENCY}, TIMEOUT={TIMEOUT}s, PROTO={PROTO}")

    dead_set = load_dead_set()
    print(f"[Boost-{PROTO}] Loaded dead list: {len(dead_set)} proxies")

    print(f"[Boost-{PROTO}] Fetching ProxRipper {PROTO.upper()} THIRD 50k ({SKIP_FIRST}-{SKIP_FIRST+MAX_PROXIES})...")
    text = ""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as sess:
            async with sess.get(
                PROXRIPPER_URL,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    text = await resp.text()
                else:
                    print(f"[Boost-{PROTO}] Fetch failed: HTTP {resp.status}")
                    return
    except Exception as e:
        print(f"[Boost-{PROTO}] Fetch error: {e}")
        return

    if not text:
        print("[Boost-{PROTO}] Empty response")
        return

    all_proxies = []
    seen = set()
    for line in text.splitlines():
        m = ADDRESS_RE.search(line)
        if m:
            p = m.group(1)
            if p not in seen:
                seen.add(p)
                all_proxies.append(p)

    proxies = all_proxies[SKIP_FIRST : SKIP_FIRST + MAX_PROXIES]
    print(f"[Boost-{PROTO}] Fetched {len(all_proxies)} total unique, using THIRD 50k: {SKIP_FIRST}-{SKIP_FIRST+len(proxies)} ({len(proxies)} proxies)")

    if not proxies:
        print("[Boost-{PROTO}] No proxies in slice, exiting")
        return

    initial_count = len(proxies)
    filtered = [p for p in proxies if p not in dead_set]
    removed_dead = initial_count - len(filtered)
    print(f"[Boost-{PROTO}] After dead filter: {initial_count} -> {len(filtered)} (removed {removed_dead} already dead)")

    if not filtered:
        print("[Boost-{PROTO}] All filtered by dead list")
        save_dead_set(dead_set)
        return

    print(f"[Boost-{PROTO}] Validating {len(filtered)} proxies (concurrency={CONCURRENCY})...")
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [test_proxy(p, semaphore) for p in filtered]
    results = await asyncio.gather(*tasks)

    working = [p for p, ok in zip(filtered, results) if ok]
    dead_new = [p for p, ok in zip(filtered, results) if not ok]

    print(f"[Boost-{PROTO}] Validation done -> Working: {len(working)}, Dead new: {len(dead_new)}")

    if dead_new:
        dead_set.update(dead_new)
    save_dead_set(dead_set)
    print(f"[Boost-{PROTO}] Dead list updated: {len(dead_set)} total (added {len(dead_new)})")

    if working:
        print(f"[Boost-{PROTO}] Geolocating {len(working)} working proxies...")
        ips = [p.split(":")[0] for p in working]
        country_map = await geolocate_batch(ips)
        print(f"[Boost-{PROTO}] Geolocated {len(country_map)} IPs")
    else:
        country_map = {}
        print("[Boost-{PROTO}] No working proxies to geolocate")

    live_data = []
    for p in working:
        ip = p.split(":")[0]
        country = country_map.get(ip, "XX")
        live_data.append({"proxy": p, "country": country})

    LIVE_FILE.write_text(
        json.dumps({"proxies": live_data, "count": len(live_data), "updated": time.time()}, indent=2),
        encoding="utf-8",
    )
    print(f"[Boost-{PROTO}] Saved live list: {LIVE_FILE} ({len(live_data)} proxies)")

    by_country = defaultdict(list)
    for entry in live_data:
        cc = entry.get("country", "XX") or "XX"
        by_country[cc].append(entry["proxy"])

    if COUNTRY_DIR.exists():
        for old in COUNTRY_DIR.glob("*"):
            if old.is_file():
                old.unlink()
            elif old.is_dir():
                for f in old.glob("*"):
                    if f.is_file():
                        f.unlink()

    for cc, plist in by_country.items():
        cc_dir = COUNTRY_DIR / cc
        cc_dir.mkdir(parents=True, exist_ok=True)
        (cc_dir / f"{PROTO}.txt").write_text("\n".join(plist) + "\n", encoding="utf-8")

    (COUNTRY_DIR / f"all_{PROTO}.txt").write_text("\n".join(working) + "\n" if working else "", encoding="utf-8")

    print("\n[Boost-{PROTO}] DONE!")
    print(f"  Working: {len(working)}")
    print(f"  Dead (new): {len(dead_new)}")
    print(f"  Dead list total: {len(dead_set)}")
    print(f"  Countries: {len(by_country)} -> {sorted(by_country.keys())[:10]}")


if __name__ == "__main__":
    asyncio.run(main())

# habibi-boost-socks5-3 - ProxRipper SOCKS5 THIRD 50k Booster

SOCKS5 proxy booster (THIRD 50k) from ProxRipper.

- **Source:** `https://raw.githubusercontent.com/Mohammedcha/ProxRipper/refs/heads/main/full_proxies/socks5.txt` - **THIRD 50k (100000-150000)**
- **Pipeline:** Load persistent dead list -> fetch THIRD 50k -> dead-first filter -> validate via `httpbin.org/ip` via `socks5://` (100 concurrency) -> update dead list (never deleted) -> geolocate working via `ip-api.com/batch` -> save `data/live_proxies.json`, `country/<CC>/socks5.txt`
- **Schedule:** Every 1 hour + manual dispatch

"""Download daily files from data.binance.vision (futures/um)."""
from __future__ import annotations

import hashlib
import os

import requests

import config

# dataset -> path segment on Vision (also the filename infix)
DATASETS = {
    "trades": "trades",
    "bookDepth": "bookDepth",
    "metrics": "metrics",
}


def file_url(dataset: str, symbol: str, date_str: str) -> str:
    seg = DATASETS[dataset]
    return f"{config.VISION_BASE}/{seg}/{symbol}/{symbol}-{seg}-{date_str}.zip"


def download(dataset: str, symbol: str, date_str: str, dest_dir: str,
             verify_checksum: bool = True) -> tuple[str, int, str] | None:
    """Download one .zip -> (path, nbytes, sha256). None if the file is 404 (no data)."""
    url = file_url(dataset, symbol, date_str)
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, os.path.basename(url))

    with requests.get(url, stream=True, timeout=(15, 300)) as r:
        if r.status_code == 404:
            return None
        r.raise_for_status()
        h = hashlib.sha256()
        n = 0
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    f.write(chunk)
                    h.update(chunk)
                    n += len(chunk)
    sha = h.hexdigest()

    if verify_checksum:
        try:
            cr = requests.get(url + ".CHECKSUM", timeout=15)
            if cr.status_code == 200:
                expected = cr.text.strip().split()[0]
                if expected and expected != sha:
                    raise ValueError(
                        f"checksum mismatch {symbol} {dataset} {date_str}: "
                        f"{sha} != {expected}")
        except requests.RequestException:
            pass  # checksum endpoint flaky -> skip verification, keep file
    return path, n, sha

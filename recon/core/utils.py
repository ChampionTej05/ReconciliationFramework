from __future__ import annotations
import hashlib
from typing import Iterable

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def synth_key(cols: Iterable[str], sep: str = '|') -> str:
    return sep.join([str(x) if x is not None else '' for x in cols])

from __future__ import annotations
from typing import Callable, Dict

comparators: Dict[str, Callable] = {}

def register_comparator(name: str):
    def deco(fn):
        comparators[name] = fn
        return fn
    return deco

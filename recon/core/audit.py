from __future__ import annotations
import json, platform, sys
from datetime import datetime
from pathlib import Path
from .utils import sha256_text

_DEF_TZ = 'Asia/Kolkata'

def write_audit(out_dir: str, config_text: str):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    audit = {
        'timestamp_ist': datetime.now().isoformat(timespec='seconds'),
        'config_sha256': sha256_text(config_text),
        'python': sys.version,
        'machine': platform.node(),
    }
    (out / 'audit.json').write_text(json.dumps(audit, indent=2))

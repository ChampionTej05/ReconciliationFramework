import argparse, json, sys
from pathlib import Path
from recon.core.pipeline import run_job
from recon.logging_setup import setup_logging, install_excepthook
import logging

def main():
    p = argparse.ArgumentParser(description="Config-driven reconciliation")
    p.add_argument('--config', required=True, help='YAML path')
    p.add_argument('--out', required=True, help='Output directory')
    p.add_argument('--backend', default='pandas', choices=['pandas'])
    p.add_argument('--log-level', default='INFO')
    args = p.parse_args()
    log_file = setup_logging(level=args.log_level, log_dir="logs")
    install_excepthook('recon.cli')
    try:
        run_job(config_path=args.config, out_dir=args.out, backend_name=args.backend)
    except:
        logging.getLogger(__name__).exception("Run Failed")
        sys.exit(1)
    finally:
        logging.shutdown()

if __name__ == '__main__':
    main()
    

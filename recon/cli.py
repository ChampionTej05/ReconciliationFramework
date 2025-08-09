import argparse, json, sys
from pathlib import Path
from recon.core.pipeline import run_job
from recon.core.pipeline import run_job

def main():
    p = argparse.ArgumentParser(description="Config-driven reconciliation")
    p.add_argument('--config', required=True, help='YAML path')
    p.add_argument('--out', required=True, help='Output directory')
    p.add_argument('--backend', default='pandas', choices=['pandas'])
    args = p.parse_args()
    run_job(config_path=args.config, out_dir=args.out, backend_name=args.backend)

if __name__ == '__main__':
    main()

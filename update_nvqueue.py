import click
import loguru as logger
from caveclient import CAVEclient
from neuvueclient import NeuvueClient
from tomlkit import parse

from cavenvq import QueueReader


@click.command()
@click.option("--config", "-c", help="Path to configuration file.")
@click.option("--dry-run", "-d", default=False, is_flag=True, help="Run without actually updating.")
def main(config, dry_run):
    qr = QueueReader(config_file=config)
    qr.run_update(dry_run=dry_run)
    if dry_run is False:
        logger.success(f"Checked nuevue queue with settings from {config}")


if __name__ == "__main__":
    main()

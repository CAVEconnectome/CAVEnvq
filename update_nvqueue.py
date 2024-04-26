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
    with open(config) as f:
        settings = parse(f.read())
    cv = CAVEclient(
        datastack_name=settings["cave"]["datastack_name"],
        server_address=settings["cave"]["server_address"],
    )
    nvq = NeuvueClient(
        nuevue_url=settings["neuvue"]["url"],
    )
    qr = QueueReader(cv, nvq)
    namespace = settings["neuvue"]["namespace"]
    extra_sieve = settings["neuvue"].get("extra_sieve", {})
    qr.run_update(nv_namespace=namespace, extra_sieve_filters=extra_sieve, dry_run=dry_run)
    if dry_run is False:
        logger.success(f"Checked nuevue queue with settings from {config}")


if __name__ == "__main__":
    main()

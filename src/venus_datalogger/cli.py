import asyncio
import logging
from argparse import ArgumentParser
import os
from typing_extensions import Annotated

import typer
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.exceptions import InfluxDBError

from ops.ecris.drivers.venus_plc import VenusPLC, VENUSController
from ops.ecris.services.venus_plc import PLCDataAquisitionService
from ops.ecris.drivers.measurement import MultiValueMeasurement

from venus_datalogger.broadcasters import broadcast_venus_data

_log = logging.getLogger("ops")
app = typer.Typer(
    help="VENUS PLC Data Aquisition Service for InfluxDB", no_args_is_help=True
)

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8181")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_DB = os.getenv("INFLUX_DB", "venus_data")


async def venus_data_loop(update_interval: float):
    if not INFLUX_TOKEN:
        _log.critical("INFLUX_TOKEN environment variable not set. Exiting.")
        return

    _log.info(f"Starting VENUS database loop. Update interval: {update_interval}s")
    _log.info(f"Connecting to InfluxDB at {INFLUX_URL}, database: {INFLUX_DB}")
    influx_client = InfluxDBClient3(
        host=INFLUX_URL, token=INFLUX_TOKEN, database=INFLUX_DB
    )

    venus_plc = VenusPLC(VENUSController(read_only=True))
    venus_data_service = PLCDataAquisitionService(
        venus_plc, update_interval=update_interval
    )

    try:
        await venus_data_service.start()
        _log.info("Data service running")

        broadcast_task = asyncio.create_task(
            broadcast_venus_data(venus_data_service.data_queue, influx_client)
        )

        _log.info("Broadcaster is running. Press Ctrl+C to exit.")
        await broadcast_task

    except (KeyboardInterrupt, asyncio.CancelledError):
        _log.info("Shutdown signal received...")
    finally:
        _log.info("Cleaning up resources...")
        influx_client.close()
        await venus_data_service.stop()
        _log.info("Cleanup complete. Exiting.")


@app.command()
def main(
    interval: Annotated[
        float, typer.Option("--interval", "-i", help="Data polling interval in seconds")
    ] = 1.0,
    debug: Annotated[
        bool, typer.Option("--debug", "-d", help="Enable debug level logging")
    ] = False,
):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    try:
        asyncio.run(venus_data_loop(update_interval=interval))
    except KeyboardInterrupt:
        _log.info("Program terminated by user.")


if __name__ == "__main__":
    app()

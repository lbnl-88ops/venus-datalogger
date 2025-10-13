import asyncio
import logging
from argparse import ArgumentParser
import os

from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.exceptions import InfluxDBError

from ops.ecris.devices.venus_plc import VenusPLC, VENUSController
from ops.ecris.services.venus_plc import PLCDataAquisitionService
from ops.ecris.model.measurement import MultiValueMeasurement

_log = logging.getLogger('ops')

INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8181')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_DB = os.getenv('INFLUX_DB', 'venus_data')

async def broadcast_venus_data(queue: asyncio.Queue, influx_client: InfluxDBClient3):
    while True:
        data: MultiValueMeasurement = await queue.get()

        point = Point('venus_plc_data').time(int(data.timestamp * 1e9))
        
        field_count = 0
        for key, value in data.values.items():
            if key.lower() == 'time':
                continue
            
            point.field(key, value)
            field_count += 1

        try:
            await asyncio.to_thread(influx_client.write, record=point)
            _log.debug(f"Successfully wrote {field_count} fields to InfluxDB.")
        except InfluxDBError as e:
            line_protocol = point.to_line_protocol()
            _log.error(f"InfluxDB API Error. Code: {e.response.status_code}. Reason: {e.response.reason}")
            _log.error(f"Failed line protocol was: {line_protocol}")
        except Exception:
            _log.exception("An unexpected error occurred during InfluxDB write.")
            
        queue.task_done()

async def venus_data_loop(update_interval: float):
    if not INFLUX_TOKEN:
        _log.critical("INFLUX_TOKEN environment variable not set. Exiting.")
        return

    _log.info(f'Starting VENUS database loop. Update interval: {update_interval}s')
    _log.info(f'Connecting to InfluxDB at {INFLUX_URL}, database: {INFLUX_DB}')
    influx_client = InfluxDBClient3(host=INFLUX_URL, token=INFLUX_TOKEN, database=INFLUX_DB)
    
    venus_plc = VenusPLC(VENUSController(read_only=True))
    venus_data_service = PLCDataAquisitionService(venus_plc, update_interval=update_interval)

    try:
        await venus_data_service.start()
        _log.info('Data service running')

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

if __name__ == '__main__':
    parser = ArgumentParser(description="VENUS PLC Data Acquisition Service for InfluxDB.")
    parser.add_argument("-i", "--interval", type=float, default=1.0, help="Data polling interval in seconds (default: 1.0)")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug level logging")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    try: 
        asyncio.run(venus_data_loop(update_interval=args.interval))
    except KeyboardInterrupt:
        _log.info('Program terminated by user.')
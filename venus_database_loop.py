import asyncio
import logging
from argparse import ArgumentParser
import os

from influxdb_client_3 import InfluxDBClient3, Point

from ops.ecris.devices.venus_plc import VENUSController

from ops.ecris.model.measurement import MultiValueMeasurement
from ops.ecris.devices import VenusPLC
from ops.ecris.services.venus_plc import PLCDataAquisitionService

_log = logging.getLogger('ops')

INFLUX_URL = os.getenv('INFLUX_URL', 'http://localhost:8181')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', 'YourSuperSecretAdminToken')
INFLUX_DB = os.getenv('INFLUX_DB', 'venus_data') 
influx_client = InfluxDBClient3(host=INFLUX_URL, token=INFLUX_TOKEN, database=INFLUX_DB)
                                

async def broadcast_venus_data(queue: asyncio.Queue):
    while True:
        data: MultiValueMeasurement = await queue.get()

        point = Point('venus_plc_data').time(int(data.timestamp * 1e9))
        for key, value in data.values.items():
            if key == 'time':
                continue
            point.field(key, value)

        try:
            await asyncio.to_thread(influx_client.write, record=point)
            _log.debug(f"Successfully wrote {len(data.values) - 1} fields to InfluxDB")
        except Exception as e:
            _log.error(f"Failed to write to InfluxDB. The problematic line protocol was: {point}")
            raise e
            
        queue.task_done()

async def venus_data_loop():
    _log.info('Starting VENUS database loop')
    _log.info(f'Connected to InfluxDBCLient at {INFLUX_URL}, database {INFLUX_DB} ')
    
    venus_plc = VenusPLC(VENUSController(read_only=False))
    
    venus_data_service = PLCDataAquisitionService(venus_plc)

    try:
        _log.info('Starting services...')
        await venus_data_service.start()

        _log.info('All services running.')

        tasks = [
            broadcast_venus_data(venus_data_service.data_queue)
        ]
        _log.info("Application is running. Press Ctrl+C to exit.")
        await asyncio.gather(*tasks)
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        _log.info("Shutdown signal received...")
    except Exception:
        _log.exception("An unknown exception occurred, shutting down...")
    finally:
        _log.info("Cleaning up resources...")
        influx_client.close()
        
        await asyncio.gather(
            venus_data_service.stop(),
            return_exceptions=True
        )
        _log.info("Cleanup complete. Exiting.")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="run with debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    try: 
        asyncio.run(venus_data_loop())
    except KeyboardInterrupt:
        print('Program terminated by user.')
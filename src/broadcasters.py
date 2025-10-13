import asyncio
from logging import getLogger

from influxdb_client_3 import InfluxDBClient3, Point, InfluxDBError

from ops.ecris.model.measurement import MultiValueMeasurement

_log = getLogger(__name__)

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
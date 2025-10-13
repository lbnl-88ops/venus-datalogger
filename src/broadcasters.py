import asyncio
from logging import getLogger

from influxdb_client_3 import InfluxDBClient3, Point, InfluxDBError

from ops.ecris.model.measurement import MultiValueMeasurement
from ops.ecris.devices.venus_plc import VENUS_PLC_DATA_DEFINITIONS as DEFS

_log = getLogger(__name__)

async def broadcast_venus_data(queue: asyncio.Queue, influx_client: InfluxDBClient3):
    while True:
        data: MultiValueMeasurement = await queue.get()

        category = "Superconductor"
        super_conductor_keys = DEFS.keys_by_category[category]
        category_table_name = category.lower().replace(' ', '_').replace('-', '_')

        point = Point('venus_plc_data').time(int(data.timestamp * 1e9))
        category_point = Point(category_table_name).time(int(data.timestamp * 1e9))
        has_general_fields = False
        has_category_fields = False
        
        field_count = 0
        for key, value in data.values.items():
            if key.lower() == 'time':
                continue
            if key.lower() in super_conductor_keys:
                category_point.field(key, value)
                has_category_fields = True
            else:
                point.field(key, value)
                has_general_fields = True
            field_count += 1
        try:
            if has_general_fields:
                await asyncio.to_thread(influx_client.write, record=point)
            if has_category_fields:
                await asyncio.to_thread(influx_client.write, record=category_point)
            _log.debug(f"Successfully wrote {field_count} fields to InfluxDB.")
        except InfluxDBError as e:
            line_protocol = point.to_line_protocol()
            _log.error(f"InfluxDB API Error. Code: {e.response.status_code}. Reason: {e.response.reason}")
            _log.error(f"Failed line protocol was: {line_protocol}")
        except Exception:
            _log.exception("An unexpected error occurred during InfluxDB write.")
            
        queue.task_done()
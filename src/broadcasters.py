import asyncio
from logging import getLogger

from influxdb_client_3 import InfluxDBClient3, Point, InfluxDBError

from ops.ecris.model.measurement import MultiValueMeasurement
from ops.ecris.devices.venus_plc import VENUS_PLC_DATA_DEFINITIONS as DEFS

_log = getLogger(__name__)

CATEGORY_MAP = {
    key.lower(): category.lower().replace(' ', '_').replace('-', '_')
    for key, category in DEFS.category_by_key.items()
}

async def broadcast_venus_data(queue: asyncio.Queue, influx_client: InfluxDBClient3):
    while True:
        data: MultiValueMeasurement = await queue.get()

        points_by_table = {}
        
        field_count = 0
        for key, value in data.values.items():
            key_lower = key.lower()
            if key_lower == 'time':
                continue
            table_name = CATEGORY_MAP.get(key_lower)
            if table_name is None:
                table_name = 'venus_plc_data'

            if table_name not in points_by_table:
                points_by_table[table_name] = Point(table_name).time(int(data.timestamp*1E9))
            points_by_table[table_name].field(key, value)
        try:
            points_to_write = list(points_by_table.values())
            await asyncio.to_thread(influx_client.write, record=points_to_write)
            _log.debug(f"Successfully wrote {field_count} fields to InfluxDB.")
        except InfluxDBError as e:
            _log.error(f"InfluxDB API Error during batch write. Code: {e.response.status_code}")
            for p in points_to_write:
                _log.error(f" -> {p.to_line_protocol()}")
        except Exception:
            _log.exception("An unexpected error occurred during InfluxDB write.")
        queue.task_done()
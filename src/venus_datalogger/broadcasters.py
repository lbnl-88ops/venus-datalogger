import asyncio
from logging import getLogger
from typing import Optional

from influxdb_client_3 import InfluxDBClient3, Point, InfluxDBError

from ops.ecris.drivers.measurement import MultiValueMeasurement
from ops.ecris.drivers.venus_plc import VENUS_PLC_DATA_DEFINITIONS as DEFS

_log = getLogger(__name__)

CATEGORY_MAP = {
    key.lower(): category.lower().replace(" ", "_").replace("-", "_")
    for key, category in DEFS.category_by_key.items()
}

TUNER_NAMES = {
    1: "Janilee", 2: "Miles", 3: "Damon",  4: "Scott", 5: "Nick", 6: "Devin", 7: "Nishi", 8: "Patrick"
}


async def broadcast_venus_data(queue: asyncio.Queue, influx_client: InfluxDBClient3):
    last_csd_status: Optional[bool] = None
    csd_start_time = None

    while True:
        data: MultiValueMeasurement = await queue.get()

        create_csd_annotation: bool = False
        csd_point = None

        if "csd_in_progress" in data.values:
            csd_in_progress = bool(data.values["csd_in_progress"])

            # Initialization
            if last_csd_status is None:
                last_csd_status = csd_in_progress
            else:
                # State change checks
                if csd_in_progress and not last_csd_status:
                    # CSD has started
                    _log.debug('CSD has started')
                    csd_start_time = int(data.timestamp * 1e9)
                elif (
                    not csd_in_progress
                    and last_csd_status
                    and csd_start_time is not None
                ):
                    _log.debug('CSD has ended')
                    if "tuner_number" in data.values:
                        current_tuner = int(data.values["tuner_number"])
                        tuner = TUNER_NAMES[current_tuner]
                    else:
                        tuner = "unknown operator"

                    # CSD ended
                    create_csd_annotation = True
                    csd_end_time = int(data.timestamp * 1e9)

                    csd_point = Point("events").time(csd_start_time)
                    csd_point.field("timeEnd", csd_end_time)
                    csd_point.tag("system", "VENUS")
                    csd_point.tag("category", "beam line")
                    csd_point.tag("type", "CSD")
                    csd_point.field("text", f"Tuner: {tuner}")
                    csd_start_time = None
            last_csd_status = csd_in_progress

        points_by_table = {}
        field_count = 0

        for key, value in data.values.items():
            key_lower = key.lower()
            if key_lower == "time":
                continue
            table_name = CATEGORY_MAP.get(key_lower)
            if table_name is None:
                table_name = "venus_plc_data"

            if table_name not in points_by_table:
                points_by_table[table_name] = Point(table_name).time(
                    int(data.timestamp * 1e9)
                )
            points_by_table[table_name].field(key, value)
            field_count += 1
        try:
            points_to_write = list(points_by_table.values())
            if create_csd_annotation:
                _log.debug("Writing CSD annotation.")
                points_to_write.append(csd_point)
            await asyncio.to_thread(influx_client.write, record=points_to_write)
            _log.debug(f"Successfully wrote {field_count} fields to InfluxDB.")
        except InfluxDBError as e:
            _log.error(
                f"InfluxDB API Error during batch write. Code: {
                    e.response.status_code}"
            )
            for p in points_to_write:
                _log.error(f" -> {p.to_line_protocol()}")
        except Exception:
            _log.exception(
                "An unexpected error occurred during InfluxDB write.")
        queue.task_done()

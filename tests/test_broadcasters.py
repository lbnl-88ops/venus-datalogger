import asyncio
import logging
from unittest.mock import MagicMock
from typing import List

import pytest
from influxdb_client_3 import Point
from influxdb_client_3.exceptions import InfluxDBError

from ops.ecris.model.measurement import MultiValueMeasurement
from src.broadcasters import broadcast_venus_data

pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_influx_client():
    client = MagicMock()
    client.write = MagicMock()
    return client


@pytest.fixture
def sample_data():
    return MultiValueMeasurement(
        source='TEST PLC',
        timestamp=12389,
        values={
            'temp': 123.45,
            'pressure': 987,
            'valve_open': True,
            'inj_i': 4814.1, # in the superconductor category
            'inj_mbar': 123.0, # in the vacuum category
            'time': 999999.99,
        }
    )


async def test_broadcast_venus_data_success(mock_influx_client, sample_data):
    queue = asyncio.Queue()
    await queue.put(sample_data)

    task = asyncio.create_task(
        broadcast_venus_data(queue, mock_influx_client)
    )

    await queue.join()
    task.cancel() # Clean up the task

    expected_tables = ['venus_plc_data', 'superconductor', 'vacuum']
    _, kwargs = mock_influx_client.write.call_args

    points_written = {}
    points_arg: List[Point] = kwargs['record']
    for point_arg in points_arg:
        line_protocol = point_arg.to_line_protocol()
        for table in expected_tables:
            if line_protocol.startswith(table):
                points_written[table] = point_arg.to_line_protocol()
                break
    assert expected_tables == list(points_written.keys())

    general_point_lp = points_written['venus_plc_data']
    assert 'temp=123.45' in general_point_lp
    assert 'pressure=987i' in general_point_lp
    assert 'valve_open=true' in general_point_lp
    assert 'inj_i' not in general_point_lp # Ensure the category key is NOT here
    assert general_point_lp.endswith(' 12389000000000') # Robustly check the timestamp

    superconductor_point_lp = points_written['superconductor']
    assert 'inj_i=4814.1' in superconductor_point_lp
    assert 'temp' not in superconductor_point_lp # Ensure general keys are NOT here
    assert superconductor_point_lp.endswith(' 12389000000000')

    vacuum_point_lp = points_written['vacuum']
    assert 'inj_mbar=123' in vacuum_point_lp
    assert 'temp' not in vacuum_point_lp # Ensure general keys are NOT here
    assert vacuum_point_lp.endswith(' 12389000000000')


async def test_broadcast_venus_data_influxdb_error(mock_influx_client, sample_data, caplog):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.reason = "Bad Request"
    mock_influx_client.write.side_effect = InfluxDBError(response=mock_response)
    
    queue = asyncio.Queue()
    await queue.put(sample_data)
    
    caplog.set_level(logging.ERROR)

    task = asyncio.create_task(
        broadcast_venus_data(queue, mock_influx_client)
    )
    await queue.join()
    task.cancel()

    mock_influx_client.write.assert_called_once()

    assert "InfluxDB API Error" in caplog.text
    assert "Code: 400" in caplog.text

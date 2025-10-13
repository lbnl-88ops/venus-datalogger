import asyncio
import logging
import time
from unittest.mock import MagicMock, AsyncMock

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
        timestamp=time.time(),
        values={
            'temp': 123.45,
            'pressure': 987,
            'valve_open': True,
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

    mock_influx_client.write.assert_called_once()

    call_args, call_kwargs = mock_influx_client.write.call_args
    assert 'record' in call_kwargs
    point_arg: Point = call_kwargs['record']
    assert point_arg._name == 'venus_plc_data'

    line_protocol = point_arg.to_line_protocol()

    assert 'time=' not in line_protocol
    assert 'temp=123.45' in line_protocol
    assert 'valve_open=true' in line_protocol
    assert 'pressure=987i' in line_protocol


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
    assert "Reason: Bad Request" in caplog.text
    assert "Failed line protocol was:" in caplog.text
import asyncio
from unittest.mock import patch, AsyncMock, ANY
import os

import pytest
from influxdb_client_3 import Point

os.environ["INFLUX_TOKEN"] = "DUMMY_TOKEN"
from venus_datalogger import venus_data_loop
from ops.ecris.devices.venus_plc import VenusPLC

MODULE = "venus_datalogger."

os.environ["INFLUX_TOKEN"] = "DUMMY_TOKEN"


@pytest.mark.asyncio
@patch(MODULE + "InfluxDBClient3")
@patch(MODULE + "VENUSController")
async def test_venus_data_loop_integrates_service_and_broadcaster(
    mock_venus_controller, mock_influx_client_constructor
):
    mock_influx_client = AsyncMock()
    mock_influx_client_constructor.return_value = mock_influx_client

    mock_venus_controller.return_value = AsyncMock()

    async def get_all_data():
        return {0: ("fcv1_ammeter", 1.23e-6), 2: ("batman_i", 2000)}

    with patch(MODULE + "VenusPLC", spec=VenusPLC) as mock_venus_plc_constructor:
        mock_plc = AsyncMock(spec=VenusPLC)
        mock_plc.get_all_data.side_effect = get_all_data
        mock_venus_plc_constructor.return_value = mock_plc

        try:
            update_interval = 0.01  # Use a very fast interval for the test
            main_task = asyncio.create_task(venus_data_loop(update_interval))
            await asyncio.sleep(update_interval * 2)
            main_task.cancel()
            await main_task
        except asyncio.CancelledError:
            pass  # This is the expected outcome of cancelling the task

    mock_influx_client.write.assert_awaited_once()

    call_args = mock_influx_client.write.call_args
    written_point = call_args.args[0]

    assert written_point._name == "venus_plc_data"
    assert written_point._fields["fcv1_ammeter"] == 1.23e-6
    assert written_point._fields["batman_i"] == 2000

import asyncio
from unittest.mock import patch, AsyncMock, ANY
import os

import pytest
from influxdb_client_3 import Point

os.environ["INFLUX_TOKEN"] = "DUMMY_TOKEN"
from src.venus_datalogger import venus_data_loop
from ops.ecris.drivers.venus_plc import VenusPLC

MODULE = "src.venus_datalogger."

os.environ["INFLUX_TOKEN"] = "DUMMY_TOKEN"


@pytest.mark.skip
@pytest.mark.asyncio
@patch(MODULE + "InfluxDBClient3")
@patch(MODULE + "VENUSController")
async def test_venus_data_loop_integrates_service_and_broadcaster(
    mock_venus_controller, mock_influx_client_constructor
):
    mock_influx_client = AsyncMock()
    mock_influx_client_constructor.return_value = mock_influx_client

    mock_venus_controller.return_value = AsyncMock()

    with (
        patch(MODULE + "VenusPLC", spec=VenusPLC) as mock_venus_plc_constructor,
        patch(MODULE + ".broadcast_venus_data") as mock_broadcaster,
        patch(
            MODULE + "PLCDataAquisitionService"
        ) as mock_aquisition_service_constructor,
    ):
        mock_venus_plc_constructor.return_value = AsyncMock(spec=VenusPLC)
        mock_broadcaster.return_value = AsyncMock()

        try:
            update_interval = 0.01  # Use a very fast interval for the test
            main_task = asyncio.create_task(venus_data_loop(update_interval))
            await asyncio.sleep(update_interval * 2)
            main_task.cancel()
            await main_task
        except asyncio.CancelledError:
            pass  # This is the expected outcome of cancelling the task

    mock_venus_plc_constructor.assert_called_once_with(mock_venus_controller)

    call_args = mock_influx_client.write.call_args
    written_point = call_args.args[0]

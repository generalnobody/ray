from ctypes import Union
import json
import logging
import asyncio
import aiohttp.web
from pydantic import BaseModel

import ray
import ray._private.services
import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    GLOBAL_GRPC_OPTIONS,
    KV_NAMESPACE_CLUSTER,
)
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.dashboard.datacenter import DataSource
from ray._private.usage.usage_constants import CLUSTER_METADATA_KEY
from ray.autoscaler._private.commands import debug_status

from ray.util.state.common import (
    ListApiOptions,
)
from ray.dashboard.state_aggregator import StateAPIManager
from ray.util.state.state_manager import (
    StateDataSourceClient,
)
from fastapi import FastAPI

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        self._ray_config = None
        DataSource.agents.signal.append(self._update_stubs)
        # TODO(fyrestone): Avoid using ray.state in dashboard, it's not
        # asynchronous and will lead to low performance. ray disconnect()
        # will be hang when the ray.state is connected and the GCS is exit.
        # Please refer to: https://github.com/ray-project/ray/issues/16328
        assert dashboard_head.gcs_address or dashboard_head.redis_address
        gcs_address = dashboard_head.gcs_address
        temp_dir = dashboard_head.temp_dir
        self.service_discovery = PrometheusServiceDiscoveryWriter(gcs_address, temp_dir)
        self._gcs_aio_client = dashboard_head.gcs_aio_client

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            options = GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True
            )
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub

    @routes.get("/api/v0/cluster_metadata")
    async def get_cluster_metadata(self, req):
        return dashboard_optional_utils.rest_response(
            success=True, message="", **self.cluster_metadata
        )

    @routes.get("/api/cluster_status")
    async def get_cluster_status(self, req):
        """Returns status information about the cluster.

        Currently contains two fields:
            autoscaling_status (str)-- a status message from the autoscaler.
            autoscaling_error (str)-- an error message from the autoscaler if
                anything has gone wrong during autoscaling.

        These fields are both read from the GCS, it's expected that the
        autoscaler writes them there.
        """
        return_formatted_output = req.query.get("format", "0") == "1"

        (legacy_status, formatted_status_string, error) = await asyncio.gather(
            *[
                self._gcs_aio_client.internal_kv_get(
                    key.encode(), namespace=None, timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for key in [
                    DEBUG_AUTOSCALING_STATUS_LEGACY,
                    DEBUG_AUTOSCALING_STATUS,
                    DEBUG_AUTOSCALING_ERROR,
                ]
            ]
        )

        formatted_status = (
            json.loads(formatted_status_string.decode())
            if formatted_status_string
            else {}
        )

        if not return_formatted_output:
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Got cluster status.",
                autoscaling_status=legacy_status.decode() if legacy_status else None,
                autoscaling_error=error.decode() if error else None,
                cluster_status=formatted_status if formatted_status else None,
            )
        else:
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Got formatted cluster status.",
                cluster_status=debug_status(formatted_status_string, error),
            )

    """
    We don't use pid and ip to get task traceback since ip and pid is bounded to a worker
    and a worker may run different tasks at different time.
    Therefore, we may provide wrong traceback info since we don't know which task is running on the worker

    Raises:
        ValueError: HTTPInternalServerError
    """

    async def get_task_info(self, option: ListApiOptions) -> Tuple[str, str, str]:
        result = await self._state_api.list_tasks(option=option)
        tasks = result.result
        state = tasks[0].get("state")
        pid = tasks[0]["worker_pid"]
        node_id = tasks[0]["node_id"]
        ip = DataSource.node_id_to_ip[node_id]
        return pid, ip, state

    @routes.get("/task/traceback")
    async def get_task_traceback(self, req) -> aiohttp.web.Response:
        logger.info(f"req {type(req)}: {req}")
        logger.info("in get_traceback")
        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")
        option = ListApiOptions(
            filters=[
                ("task_id", "=", task_id),
                ("attempt_number", "=", attempt_number),
            ],
            detail=True,
            timeout=10,
        )
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self._dashboard_head.gcs_aio_client
        )
        self._state_api = StateAPIManager(self._state_api_data_source_client)
        logger.info(f"self._state_api {type(self._state_api)}: {self._state_api}")

        result = await self._state_api.list_tasks(option=option)
        tasks = result.result
        logger.info(f"tasks {type(tasks)}: {tasks}")

        pid = tasks[0]["worker_pid"]
        node_id = tasks[0]["node_id"]
        ip = DataSource.node_id_to_ip[node_id]
        logger.info(f"pid {type(pid)}: {pid}")
        logger.info(f"node_id {type(node_id)}: {node_id}")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        logger.info(
            "Sending stack trace request to {}:{} with native={}".format(
                req.query.get("ip"), pid, native
            )
        )
        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(pid=pid, native=native)
        )

        ## Check if the task attempt is still running
        new_result = await self._state_api.list_tasks(option=option)
        new_tasks = new_result.result
        logger.info(f"new_tasks {type(new_tasks)}: {new_tasks}")
        state = new_tasks[0].get("state")
        new_pid = new_tasks[0]["worker_pid"]
        logger.info(f"new_pid {type(new_pid)}: {new_pid}")
        new_node_id = new_tasks[0]["node_id"]
        logger.info(f"new_node_id {type(new_node_id)}: {new_node_id}")
        new_ip = DataSource.node_id_to_ip[new_node_id]
        logger.info(f"new_ip {type(new_ip)}: {new_ip}")

        if new_pid is None or new_ip is None:
            raise ValueError(
                f"pid or ip is None, could not fetch the info you need: {pid}, {ip}"
            )

        if pid != new_pid or ip != new_ip:
            raise ValueError(
                f"The worker begin to work on other task, you could not get the correct CPU profile info"
            )
        if state != "RUNNING":
            raise ValueError(
                f"The task attempt is not running: the current state is {state}."
            )

        if reply.success:
            logger.info("Returning stack trace, size {}".format(len(reply.output)))
            return aiohttp.web.Response(text=reply.output)
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    """
    We don't use pid and ip to get task cpu profile since ip and pid is bounded to a worker
    and a worker may run different tasks at different time.
    Therefore, we may provide wrong traceback info since we don't know which task is running on the worker

    Raises:
        ValueError, HTTPInternalServerError
    """

    @routes.get("/task/cpu_profile")
    async def get_task_cpu_profile(self, req) -> aiohttp.web.Response:
        logger.info(f"req {type(req)}: {req}")
        logger.info("before self._state_api.list_tasks")
        if "task_id" not in req.query:
            raise ValueError("task_id is required")
        if "attempt_number" not in req.query:
            raise ValueError("task's attempt number is required")
        task_id = req.query.get("task_id")
        attempt_number = req.query.get("attempt_number")

        option = ListApiOptions(
            filters=[
                ("task_id", "=", task_id),
                ("attempt_number", "=", attempt_number),
            ],
            detail=True,
            timeout=10,
        )

        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._state_api_data_source_client = StateDataSourceClient(
            gcs_channel, self._dashboard_head.gcs_aio_client
        )
        self._state_api = StateAPIManager(self._state_api_data_source_client)
        logger.info(f"self._state_api {type(self._state_api)}: {self._state_api}")

        result = await self._state_api.list_tasks(option=option)
        tasks = result.result
        logger.info(f"tasks {type(tasks)}: {tasks}")

        pid = tasks[0]["worker_pid"]
        node_id = tasks[0]["node_id"]
        ip = DataSource.node_id_to_ip[node_id]
        logger.info(f"pid {type(pid)}: {pid}")
        logger.info(f"node_id {type(node_id)}: {node_id}")

        if not pid or not ip:
            raise ValueError(
                f"pid or ip is None, could not fetch the info you need: {pid}, {ip}"
            )

        duration = int(req.query.get("duration", 5))
        if duration > 60:
            raise ValueError(f"The max duration allowed is 60: {duration}.")
        format = req.query.get("format", "flamegraph")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        reporter_stub = self._stubs[ip]
        logger.info(f"reporter_stub {type(reporter_stub)}: {reporter_stub}")

        logger.info(
            "Sending CPU profiling request to {}:{} for {} with native={}".format(
                ip, pid, task_id, native
            )
        )

        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid, duration=duration, format=format, native=native
            )
        )
        logger.info(f"reply {type(reply)}: {reply}")

        ## Check if the task attempt is still running
        new_result = await self._state_api.list_tasks(option=option)
        new_tasks = new_result.result
        logger.info(f"new_tasks {type(new_tasks)}: {new_tasks}")
        state = new_tasks[0].get("state")
        new_pid = new_tasks[0]["worker_pid"]
        logger.info(f"new_pid {type(new_pid)}: {new_pid}")
        new_node_id = new_tasks[0]["node_id"]
        logger.info(f"new_node_id {type(new_node_id)}: {new_node_id}")
        new_ip = DataSource.node_id_to_ip[new_node_id]
        logger.info(f"new_ip {type(new_ip)}: {new_ip}")

        if new_pid is None or new_ip is None:
            raise ValueError(
                f"pid or ip is None, could not fetch the info you need: {pid}, {ip}"
            )

        if pid != new_pid or ip != new_ip:
            raise ValueError(
                f"The worker begin to work on other task, you could not get the correct CPU profile info"
            )
        if state != "RUNNING":
            raise ValueError(
                f"The task attempt is not running: the current state is {state}."
            )
        if reply.success:
            logger.info(
                "Returning profiling response, size {}".format(len(reply.output))
            )
            return aiohttp.web.Response(
                body=reply.output,
                headers={
                    "Content-Type": "image/svg+xml"
                    if format == "flamegraph"
                    else "text/plain"
                },
            )
        return aiohttp.web.HTTPInternalServerError(
            text="Could not find CPU Flame Graph info for task {}".format(task_id)
        )

    @routes.get("/worker/traceback")
    async def get_traceback(self, req) -> aiohttp.web.Response:
        if "ip" in req.query:
            reporter_stub = self._stubs[req.query["ip"]]
        else:
            reporter_stub = list(self._stubs.values())[0]
        pid = int(req.query["pid"])
        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        logger.info(
            "Sending stack trace request to {}:{} with native={}".format(
                req.query.get("ip"), pid, native
            )
        )
        reply = await reporter_stub.GetTraceback(
            reporter_pb2.GetTracebackRequest(pid=pid, native=native)
        )
        if reply.success:
            logger.info("Returning stack trace, size {}".format(len(reply.output)))
            return aiohttp.web.Response(text=reply.output)
        else:
            return aiohttp.web.HTTPInternalServerError(
                text="Could not find trackback for task {}".format(task_id)
            )

    @routes.get("/worker/cpu_profile")
    async def cpu_profile(self, req) -> aiohttp.web.Response:
        if "ip" in req.query:
            reporter_stub = self._stubs[req.query["ip"]]
        else:
            reporter_stub = list(self._stubs.values())[0]
        pid = int(req.query["pid"])
        duration = int(req.query.get("duration", 5))
        if duration > 60:
            raise ValueError(f"The max duration allowed is 60: {duration}.")
        format = req.query.get("format", "flamegraph")

        # Default not using `--native` for profiling
        native = req.query.get("native", False) == "1"
        logger.info(
            "Sending CPU profiling request to {}:{} with native={}".format(
                req.query.get("ip"), pid, native
            )
        )
        reply = await reporter_stub.CpuProfiling(
            reporter_pb2.CpuProfilingRequest(
                pid=pid, duration=duration, format=format, native=native
            )
        )
        if reply.success:
            logger.info(
                "Returning profiling response, size {}".format(len(reply.output))
            )
            return aiohttp.web.Response(
                body=reply.output,
                headers={
                    "Content-Type": "image/svg+xml"
                    if format == "flamegraph"
                    else "text/plain"
                },
            )
        else:
            return aiohttp.web.HTTPInternalServerError(text=reply.output)

    async def run(self, server):
        # Need daemon True to avoid dashboard hangs at exit.
        self.service_discovery.daemon = True
        self.service_discovery.start()
        gcs_addr = self._dashboard_head.gcs_address
        subscriber = GcsAioResourceUsageSubscriber(address=gcs_addr)
        await subscriber.subscribe()
        cluster_metadata = await self._dashboard_head.gcs_aio_client.internal_kv_get(
            CLUSTER_METADATA_KEY,
            namespace=KV_NAMESPACE_CLUSTER,
        )
        self.cluster_metadata = json.loads(cluster_metadata.decode("utf-8"))

        while True:
            try:
                # The key is b'RAY_REPORTER:{node id hex}',
                # e.g. b'RAY_REPORTER:2b4fbd...'
                key, data = await subscriber.poll()
                if key is None:
                    continue
                data = json.loads(data)
                node_id = key.split(":")[-1]
                DataSource.node_physical_stats[node_id] = data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from reporter agent."
                )

    @staticmethod
    def is_minimal_module():
        return False

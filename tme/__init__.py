"""
Simple module to allow function re-use across Nutanix
v4 SDK code samples
"""

import time
import urllib3
from dataclasses import dataclass
from timeit import default_timer as timer

import ntnx_prism_py_client
from ntnx_prism_py_client import ApiClient as PrismClient
from ntnx_prism_py_client import Configuration as PrismConfiguration

from ntnx_prism_py_client.models.prism.v4.config.TaskGetResponse import (
    TaskGetResponse
)


@dataclass
class Config:
    """
    dataclass to hold configuration for each script run
    nice and modular
    """

    pc_ip: str
    pc_username: str
    pc_password: str


class Utils:
    """
    class to manage simple reusable functions across the Python
    v4 API code samples
    """

    prism_config: PrismConfiguration

    def __init__(self, pc_ip: str, username: str, password: str):
        """
        class constructor
        create reusable instances of Prism connections (etc)
        """
        self.prism_config = PrismConfiguration()
        self.prism_config.host = pc_ip
        self.prism_config.username = username
        self.prism_config.password = password
        self.prism_config.verify_ssl = False
        self.prism_client = PrismClient(configuration=self.prism_config)
        self.prism_client.add_default_header(
            header_name="Accept-Encoding", header_value="gzip, deflate, br"
        )
        self.prism_instance = ntnx_prism_py_client.api.TaskApi(
            api_client=self.prism_client
        )
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def get_task(self, ext_id: str) -> TaskGetResponse:
        """
        method used to get details of a specified task
        """
        task = self.prism_instance.task_get(f"ZXJnb24=:{ext_id}")
        if task:
            return task
        return None

    def confirm(self, message: str):
        """
        method to request a yes/NO confirmation from the user
        used to run or skip precheck, inventory (etc)
        """
        yes_no = input(f"{message} (yes/NO): ").lower()
        return yes_no == "yes"

    def monitor_task(
        self, task_ext_id, task_name, pc_ip, username, password, poll_timeout, prefix = "ZXJnb24=:"
    ):
        """
        method used to monitor Prism Central tasks
        will print a series of period characters and re-check task
        status at the specified interval
        this version uses the Prism SDK
        """
        start = timer()
        # print message until specified  task is finished
        prism_config = PrismConfiguration()
        prism_config.host = pc_ip
        prism_config.username = username
        prism_config.password = password
        prism_config.verify_ssl = False
        prism_client = PrismClient(configuration=prism_config)
        prism_client.add_default_header(
            header_name="Accept-Encoding", header_value="gzip, deflate, br"
        )
        prism_instance = ntnx_prism_py_client.api.TaskApi(api_client=prism_client)
        task = prism_instance.task_get(f"{prefix}{task_ext_id}")
        units = "second" if poll_timeout == 1 else "seconds"
        print(
            f"{task_name} running, checking progress every {poll_timeout} {units} ...",
            end="",
        )
        while True:
            if task.data.status == "RUNNING":
                print(".", end="", flush=True)
            else:
                print(" finished.")
                break
            time.sleep(int(poll_timeout))
            task = prism_instance.task_get(f"{prefix}{task_ext_id}")
        end = timer()
        elapsed_time = end - start
        if elapsed_time <= 60:
            duration = f"{round(elapsed_time, 0)} seconds"
        else:
            duration = f"{round(elapsed_time // 60, 0)} minutes"
        return duration

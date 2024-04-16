import logging
import os
import sys
import getopt

import ntnx_prism_py_client
import psutil
import urllib3
import json
import getpass
import requests
import socket
from requests.auth import HTTPBasicAuth
from base64 import b64encode
import pexpect
from pexpect import pxssh
from pathlib import Path
from packaging.version import Version
import inspect

import ntnx_lcm_py_client
from ntnx_lcm_py_client.rest import ApiException as LCMException

from ntnx_lcm_py_client.Ntnx.lcm.v4.common.PrecheckSpec import PrecheckSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.EntityUpdateSpec import EntityUpdateSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.EntityUpdateSpecs import EntityUpdateSpecs
from ntnx_lcm_py_client.Ntnx.lcm.v4.resources.RecommendationSpec import RecommendationSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.UpdateSpec import UpdateSpec

import configparser

import multiprocessing
from multiprocessing import Manager

from datetime import datetime
from sys import exit
from time import sleep

from tme import Utils

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

version = '1.1.0'
filename = os.path.basename(os.path.realpath(__file__))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

proxies = {'http': '', 'https': ''}

print("******************************************************")
print("* Nutanix " + filename)
print("* Version " + version)
print("******************************************************")

# ========================================================================================================================================================================================== CONFIG FILE

config = configparser.ConfigParser()
config_file = str(dir_path) + '/srv_config.txt'
config.read_file(open(config_file))

prism_username = config.get('USER', 'prism_username')
cli_username = config.get('USER', 'cli_username')
domain_suffix = config.get('USER', 'domain_suffix')

upgrade_attempts = config.get('UPGRADE', 'upgrade_attempts')
upgrade_method = config.get('UPGRADE', 'upgrade_method')
aos_build = config.get('UPGRADE', 'aos_build')

proxy_scheme = config.get('PROXY', 'proxy_scheme')
proxy_host = config.get('PROXY', 'proxy_host')
proxy_port = config.get('PROXY', 'proxy_port')

logfile = config.get('SETTINGS', 'logger_file')
poll_timeout = config.get('SETTINGS', 'poll_timeout')


# ============================================================================================================================================================================================== Utility


def convert_time(task_time_microseconds):
    task_time_seconds = task_time_microseconds / 1000000
    task_time = datetime.fromtimestamp(task_time_seconds)

    current_time = datetime.now()

    time_diff = current_time - task_time
    age_hours = round(time_diff.total_seconds() / 3600)

    return age_hours


def is_ip(ip):
    try:
        socket.inet_aton(ip)
        return True

    except:
        return False


def check_ping(address):
    try:
        response = os.system("ping -c 1 " + address + " > /dev/null")
        # print('=====', response)
        # and then check the response...
        if response == 0:
            pingstatus = True  # "Network Active"
        else:
            pingstatus = False  # "Network Error"

        return pingstatus

    except:
        return False


# ============================================================================================================================================================================================== Connect


def connect_ssh(srv, cmd, username, password):  # Uses PE User and Pass

    options = dict(StrictHostKeyChecking="no", UserKnownHostsFile="/dev/null")
    s = pxssh.pxssh(timeout=300, options=options)

    try:
        if not s.login(srv, username, password):
            return 'FAILED'
        else:
            s.sendline(cmd)
            s.prompt()  # match the prompt
            # print('=====', str(srv), s.before.decode('UTF-8'))  # decode to string and print everything before the prompt.
            s.logout()
            return 'DONE'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        job_status[str(srv)] = 'FAILED: ' + str(inspect.currentframe().f_code.co_name)
        return 'FAILED'


def execute_api(req_type, srv, auth, api_endpoint, payload):
    api_url = 'https://' + str(srv).strip() + ':9440/' + api_endpoint
    payload = json.dumps(payload)
    headers = {
        'Authorization': auth,
        'Content-Type': 'application/json'
    }

    # print('=====', 'api_url', api_url)
    # print('=====', 'payload', payload)
    # print('=====', 'headers', headers)

    try:
        if req_type == 'post':
            response = requests.post(url=api_url, proxies=proxies, verify=False,
                                     data=payload,
                                     headers=headers
                                     )

            # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response POST', str(json_response))

            try:
                json_response = response.json()
                return json_response

            except Exception as msg:
                return 'FAILED'

        elif req_type == 'get':
            response = requests.get(url=api_url, proxies=proxies, verify=False,
                                    data=payload,
                                    headers=headers
                                    )

            # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response GET', str(json_response))

            try:
                json_response = response.json()
                return json_response

            except Exception as msg:
                return 'FAILED'

        else:
            return {'ERROR': 'API Failed'}

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def connect_ntnx_lcm_client(srv):
    config = ntnx_lcm_py_client.configuration.Configuration()
    config.host = str(srv)
    config.port = 9440
    config.verify_ssl = False
    config.max_retry_attempts = 1
    config.backoff_factor = 3
    config.username = str(prism_username)
    config.password = str(prism_password)

    client = ntnx_lcm_py_client.ApiClient(configuration=config)

    return client


# ============================================================================================================================================================================================= Get Info


def get_cluster_info(srv):
    try:
        api_endpoint = 'api/nutanix/v2.0/cluster/'

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        try:
            uuid = json_response['cluster_uuid']
            return uuid

        except Exception as msg:
            return 'FAILED'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def get_cmv_ips(srv):
    try:
        api_endpoint = 'PrismGateway/services/rest/v2.0/hosts/'

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        entities = json_response['entities']
        cvm_ips = []

        for x in entities:
            ip = x['controller_vm_backplane_ip']
            cvm_ips.append(str(ip).strip())

        return cvm_ips

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def get_cluster_build(srv):
    try:
        id = '1708238042442'
        api_endpoint = 'PrismGateway/services/rest/v1/cluster/version?__=' + str(id)

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        ver = str(json_response['version'])
        build = ver.split('-')
        build_number = str(build[1])

        return build_number

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def check_disks(srv):
    try:

        api_endpoint = 'PrismGateway/services/rest/v2.0/hosts/'

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        cluster = json_response['entities']

        results_list = []

        for host in cluster:
            hostname = host['name']

            for disk, info in host['disk_hardware_configs'].items():
                location = disk

                try:
                    serial = info['serial_number']
                    bad = info['bad']
                    mounted = info['mounted']
                    vendor = info['vendor']
                    model = info['model']

                    if bad:
                        var = str(hostname) + '\tSlot: ' + str(location) + '\tBad: ' + str(bad) + '\tMounted: ' + str(mounted) + '\tSerial: ' + str(serial) + '\tVendor: ' + str(vendor) + '\tModel: ' + str(model)
                        results_list.append(var)
                        # print(hostname, 'Slot:', location, 'Bad:', bad, 'Mounted:', mounted, 'Serial:', serial, 'Vendor:', vendor, 'Model:', model)

                except:
                    continue

        return results_list

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'

# ========================================================================================================================================================================================= Active Loops


def record_status(job_status, logging):
    sleep(30)

    while True:

        print('****************************************************** STATUS ***** START')

        for key, value in job_status.items():
            print('Server: ' + str(key), 'Status: ' + str(value))
            logging.critical('Server: ' + str(key) + ' Status: ' + str(value))

        print('****************************************************** STATUS ******* END')

        sleep(60)  # 1 min


def loop(srv, job_status, logging):

    try:

        # ===== Ping Check ===== Start
        pingable = check_ping(srv)
        if not pingable:
            job_status[srv] = 'FAILED: Ping Test - Quitting'
            return
        # ===== Ping Check ===== End

        # ===== Password Check ===== Start
        status = connect_ssh(srv, "echo 'I am Connected'", cli_username, cli_password)
        if status == 'FAILED':
            note = 'FAILED: Password Check - CLI Account - ' + str(cli_username) + ' - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)
            return

        sleep(2)

        status = get_cluster_info(srv)
        if status == 'FAILED':
            note = 'FAILED: Password Check - Prism Account - ' + str(prism_username) + ' - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)
            return
        # ===== Password Check ===== End

        sleep(5)

        # ===== Check for bad disk ===== Start

        try:
            result = check_disks(srv)
            with open('bad_disk_list.txt', 'a') as file:
                for x in result:
                    file.write(x + '\n')

            note = 'Complete: check_disks - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

        except:
            note = 'FAILED: check_disks - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

        # ===== Check for bad disk ===== Start

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)


if __name__ == "__main__":

    # ======================================================================================================================================================================================== Load List
    try:

        try:
            list_pc = open("PC_List.txt", "r")
        except Exception as error:
            print('ERROR', error)

        try:
            # print(sys.argv)
            opts, args = getopt.getopt(sys.argv[1:], "h:c:", ["cfile="])

            for opt, arg in opts:
                # print(opt)

                if opt == '-h':
                    print('test.py -c <clusterlist.txt>')
                    sys.exit()

                elif opt in ("-c", "--cfile"):
                    list_cluster = open(str(arg), "r")

        except getopt.GetoptError:
            print('test.py -c <clusterlist.txt>')
            sys.exit(2)

        try:

            prism_password = getpass.getpass(
                prompt='Please enter your Prism Element password: ',
                stream=None,
            )

            cli_password = getpass.getpass(
                prompt='Please enter your CLI password: ',
                stream=None,
            )

            prism_encoded_credentials = b64encode(bytes(f"{prism_username}:{prism_password}", encoding="ascii")).decode("ascii")
            prism_auth_header = f"Basic {prism_encoded_credentials}"

            cli_encoded_credentials = b64encode(bytes(f"{cli_username}:{cli_password}", encoding="ascii")).decode("ascii")
            cli_auth_header = f"Basic {cli_encoded_credentials}"

            # print(encoded_credentials, auth_header)

        except Exception as error:
            print('ERROR', error)
            sys.exit()

        # =========================================================================================================================================================================== Load Log File Settings

        logging.basicConfig(filename=logfile,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

        # ======================================================================================================================================================================================= Start Loop

        print('****************************************************** Start')

        logging.info("Script Starting")

        jobs = []
        job_pid = []

        manager = Manager()
        job_status = manager.dict()

        process = multiprocessing.Process(target=record_status, args=(job_status, logging))
        jobs.append(process)

        with open('bad_disk_list.txt', 'w') as file:
            file.write('')

        for x in list_cluster:

            x = str(x).strip()

            if not is_ip(x):
                if domain_suffix not in x:
                    x = str(x) + '.' + domain_suffix

            job_status[str(x)] = 'Starting'

            if len(x) > 1:
                process = multiprocessing.Process(target=loop, args=(x, job_status, logging))
                jobs.append(process)

        for j in jobs:
            j.start()

        for j in jobs:
            j.join()

        print('****************************************************** End')

    except KeyboardInterrupt:
        print('Attempting to close all threads')

        for j in jobs:
            j.kill()

        print('Closed')

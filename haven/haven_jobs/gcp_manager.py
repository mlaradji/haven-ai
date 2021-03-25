from .. import haven_utils as hu
from .. import haven_chk as hc
import os
from typing import TypedDict, List

import uuid
import json
import time
import copy
import pandas as pd
import numpy as np
import getpass
import pandas as pd
from subprocess import SubprocessError

from oauth2client.client import GoogleCredentials
from googleapiclient import discovery
from googleapiclient import errors
from loguru import logger

GoogleCloudAIPlatformJobList = List[dict]


class GoogleJobConfig(TypedDict):
    JOB_NAME: str
    PACKAGE_PATH: str
    MODULE_NAME: str
    JOB_DIR: str
    PROJECT_ID: str  # Google Cloud project id.
    REGION: str  # Google Cloud region.


def generate_job_id() -> str:
    """
    Generates a random string that starts with a letter and contains only letters, numbers, and underscores, as per Google Cloud Platform's requirements.
    """
    return f"haven_ai_{uuid.uuid4().hex}"


# Job submission
# ==============
def submit_job(
    api,
    account_id: str,
    command,
    job_config: GoogleJobConfig,
    workdir: str,
    savedir_logs=None,
):
    """
    Submit a job to Google AI Platform through the `gcloud` executable. This function requires the Google SDK to be installed, and the local path to the Google credentials should be set in the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable.

    You might need to execute `gcloud auth login` before using this function.
    """
    # Generate a random id. ?: Should we use the same id as the internal haven-ai id?
    job_id = generate_job_id()

    submit_command = f"gcloud ai-platform jobs submit training {job_id} \
        --scale-tier basic \
        --package-path {workdir} \
        --project {job_config['PROJECT_ID']} \
        --module-name {job_config['MODULE_NAME']} \
        --job-dir {job_config['JOB_DIR']} \
        --region {job_config['REGION']} \
        --runtime-version 2.4 \
        --python-version 3.7"

    while True:
        try:
            status = hu.subprocess_call(submit_command).split()[-1]
        except SubprocessError as e:
            if "Socket timed out" in str(e.output):
                print("sbatch time out and retry now")
                time.sleep(1)
                continue
            else:
                # other errors
                exit(str(e.output)[2:-1].replace("\\n", ""))
        assert status == "QUEUED", status  # ?: Necessary?
        break

    return job_id


# Job status
# ===========
def get_job(api, job_id: str):
    """Get job information."""
    job_info = get_jobs_dict(None, [job_id])[job_id]
    job_info["job_id"] = job_id

    return job_info


def get_jobs(api, account_id: str) -> GoogleCloudAIPlatformJobList:
    """
    Get all Google Cloud AI-Platform jobs in the project with project id `account_id`.
    """

    command = f"gcloud ai-platform jobs list --format=json --project={account_id}"
    while True:
        try:
            job_list = json.loads(hu.subprocess_call(command))
        except SubprocessError as e:
            if "Socket timed out" in str(e.output):
                print("squeue time out and retry now")
                time.sleep(1)
                continue
            else:
                # other errors
                exit(str(e.output)[2:-1].replace("\\n", ""))
        except json.JSONDecodeError as e:
            logger.exception(e)
            break
        break

    # result = [
    #     {"job_id": j.split()[0], "state": j.split()[1]}
    #     for j in job_list.split("\n")[1:-1]
    # ]
    # return result
    return job_list


def get_jobs_dict(api, job_id_list: List[str], query_size=None):
    """
    Return a job_id -> job dictionary for all job_id's in `job_id_list`.
    """
    command = f"gcloud ai-platform jobs list --filter='jobId:({','.join(job_id_list)})' --format=json"
    while True:
        try:
            job_list: GoogleCloudAIPlatformJobList = json.loads(
                hu.subprocess_call(command)
            )
        except SubprocessError as e:
            if "Socket timed out" in str(e.output):
                print("squeue time out and retry now")
                time.sleep(1)
                continue
            else:
                # other errors
                exit(str(e.output)[2:-1].replace("\\n", ""))
        except json.JSONDecodeError as e:
            logger.exception(e)
            break
        break

    # We have a list of jobs. Let's convert the list to a list of job id's.
    job_dict = {}
    for job in job_list:
        # Add some fields that Haven AI jobs expects.
        assert "state" in job, job
        job.update(
            {
                "job_id": job["jobId"],
                "runs": [],  # ?: Can we get this from somewhere, or does each GCP job only run once?
                "command": job["trainingInput"],
            }
        )
        job_dict[job["jobId"]] = job

    return job_dict


# Job kill
# ===========


def kill_job(api, job_id: str):
    """Kill a job job until it is dead."""
    job = get_job(api, job_id)

    if job["state"] in ["CANCELLED", "COMPLETED", "FAILED", "TIMEOUT"]:
        logger.info("Job '{}' is already dead", job_id)
    else:
        kill_command = "gcloud ai-platform jobs cancel {job_id}"
        while True:
            try:
                hu.subprocess_call(kill_command)
                logger.info("%s CANCELLING..." % job_id)
            except Exception as e:
                if "Socket timed out" in str(e):
                    logger.error("scancel time out and retry now")
                    time.sleep(1)
                    continue
            break

        # confirm cancelled
        job = get_job(api, job_id)
        while job["state"] != "CANCELLED":
            time.sleep(2)
            job = get_job(api, job_id)

        logger.success("%s now is dead." % job_id)

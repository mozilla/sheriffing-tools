# Calculate the average time between a failed CI task finishing and it getting
# classified.
# LIMITATIONS:
# - Is not aware of retriggers (task gets cloned on the same push and run) and
#   backfills (task gets added to previous pushes) which are used to identify
#   if an issue is permanent or frequent or which push causes it.
# For this reason, the longest classification times are not taken into account
# but the highest percentiles are ignored (see below for threshold).


def log_debug(message):
    if DEBUG:
        print(message)

def value_from_list(list, key):
    return list[header_to_index[key]]

import argparse
import json
import urllib.request
from datetime import datetime

DATA_SOURCE_QUERY_ID = 78112

parser = argparse.ArgumentParser(description='Calculate average time for task classifications on sheriffed trees.')

# API key for access to the query data on https://sql.telemetry.mozilla.org
# By default only available to Mozilla employees. On the query page, open its
# menu, call the menu item "Show API Key" and copy the string after the
# "api_key=" in the url.
parser.add_argument('--key',
                    metavar='api_key',
                    type=str,
                    required=True,
                    help='API key for https://sql.telemetry.mozilla.org')

# Percentage of fastest response times to use between 0 and 100. Slower
# ones will be ignored. E.g. reclassifications create slow times (old
# classification gets deleted).
parser.add_argument('--debug',
                    action='store_true',
                    help="Turn on debug logging")

# Percentage of fastest response times to use between 0 and 100. Slower
# ones will be ignored. E.g. reclassifications create slow times (old
# classification gets deleted).
parser.add_argument('--percent', type=int, default=95,
                    help="Percentage of fastest response times to use (int, 0..100)")

# Time in seconds in which the job should be classified. If retriggers start
# not after this time limit after the task ended, the time delta between initial
# task failure and classification will be part of the calculation.
parser.add_argument('--response-limit', type=int, default=15*60,
                    help="Time in seconds in which the job should be classified (int)")

# Maximum time after a push in which a job has to start to be taken into
# account. Used to exclude manually requested jobs (retriggers, backfills)
# which might not be shown anymore on the jobs watched by the sheriffs
# because they regard the push as done. Time is in seconds.
parser.add_argument('--start-delay', type=int, default=4*60*60,
                    help="Maximum time after a push in which a job has to start (int)")

args = parser.parse_args()
query_args = vars(args)

DEBUG = query_args["debug"]

DATA_SOURCE_API_KEY = query_args["key"]

# The .csv format gets used because the order of rows is important - adjacent
# rows are used for calculations.
DATA_SOURCE_URL = ("https://sql.telemetry.mozilla.org/api/queries/" +
                   str(DATA_SOURCE_QUERY_ID) +
                   "/results.json?api_key=" +
                   DATA_SOURCE_API_KEY)
log_debug(("DATA_SOURCE_URL:", DATA_SOURCE_URL))

CLASSIFICATION_DELAY_MAX = 24 * 60 * 60

# TODO: support passing start and end date into query
# parser = RecipeParser('date')

PERCENTAGE_TO_USE = query_args["percent"]
RESPONSE_LIMIT = query_args["response_limit"]
JOB_START_DELAY_MAX = query_args["start_delay"]

# A failed job can be classified or be checked for its intermittance with
# retriggers. Waiting for those retriggers should not be counted against the
# classification time but sql.telemetry.mozilla.org is not able to export
# this data (query timeout)

# A job group is the set of all job runs which have the push, platform and
# job configuration in common. By default, this is 1 (or 0), unless a job
# gets retriggers or backfilled (or automatically retried, e.g. because the
# machine got terminated by the machine pool provider - irrelevant here).
jobGroups = []
jobGroup = {"repositoryName": None,
            "pushRevision": None,
            "jobName": None, # concatenation of platform and test suite config
            "jobs": []}

data_request = urllib.request.urlopen(DATA_SOURCE_URL, timeout=60)

data = json.load(data_request)
rows = data["query_result"]["data"]["rows"]

data_row_next = rows[0]
for row_pos in range(len(rows)):
    data_row = data_row_next
    data_row_next = rows[row_pos]
    jobGroupEndsWithRow = False
    if row_pos == len(rows) - 1:
        jobGroupEndsWithRow = True
    else:
        if (data_row["repository_id"] != data_row_next["repository_id"] or
            data_row["push_id"] != data_row_next["push_id"] or
            data_row["job_type_name"] != data_row_next["job_type_name"]):
            # Next row contains new job group.
            jobGroupEndsWithRow = True
    if jobGroup["repositoryName"] is None:
        # Set up job group.
        jobGroup["repositoryName"] = data_row["repository_name"]
        jobGroup["pushRevision"] = data_row["push_revision"]
        jobGroup["jobName"] = data_row["job_type_name"]
    jobGroup["jobs"].append({# Timestamp of the push
                             'repo.push.date': datetime.strptime(data_row["push_time"], "%Y-%m-%dT%H:%M:%S").timestamp(),
                             # Type of the failure classification, e.g. "intermittent", "fixed by commit"
                             'failure.notes.failure_classification': data_row["classification_name"],
                             # Timestamp of the creation of the failure classification 
                             'failure.notes.created': datetime.strptime(data_row["classification_timestamp"], "%Y-%m-%dT%H:%M:%S.%f").timestamp(),
                             # Timestamp of the job's start time
                             'action.start_time': datetime.strptime(data_row["job_start_time"], "%Y-%m-%dT%H:%M:%S").timestamp(),
                             # Timestamp of the job's end time (int)
                             'action.end_time': datetime.strptime(data_row["job_end_time"], "%Y-%m-%dT%H:%M:%S").timestamp()})
    if jobGroupEndsWithRow:
        jobGroups.append(jobGroup)
        jobGroup = {"repositoryName": None,
                    "pushRevision": None,
                    "jobName": None, # concatenation of platform and test suite config
                    "jobs": []}

# Ignore each job group which at least one job which has been classified as "fixed by commit".
jobGroupsFiltered = list(
                        filter(
                            lambda jobGroup:
                                len(
                                    list(
                                        filter(
                                            lambda job:
                                                job['failure.notes.failure_classification'] == "fixed by commit",
                                            jobGroup["jobs"]
                                        ),
                                    )
                                ) == 0,
                        jobGroups)
                    )

# Holds all the response time for failures. The highest ones will get
# ignored, e.g. for reclassifications.
classificationTimedeltas = []
for jobGroup in jobGroupsFiltered:
    jobGroup["jobs"].sort(key=lambda job: job["action.start_time"])
    # lastTimeOk holds the end time of the last job which started before an
    # inactivity gap bigger than RESPONSE_LIMIT
    lastTimeOk = None
    for job in jobGroup["jobs"]:
        if not lastTimeOk:
            lastTimeOk = job["action.end_time"]
        else:
            # RESPONSE_LIMIT threshold in which action must be taken
            if job["action.start_time"] - lastTimeOk <= RESPONSE_LIMIT:
                lastTimeOk = job["action.end_time"]
            else:
                # Found a gap
                break
    # Filter out jobs which have started more than the allowed time after the push
    jobsNormalTime = []
    for job in jobGroup["jobs"]:
        # Ignore pushes that don't have meta data for the push datetime.
        if not job["repo.push.date"]:
            continue
        if job["action.start_time"] - job["repo.push.date"] <= JOB_START_DELAY_MAX:
            jobsNormalTime.append(job)
    # jobsNormalTime = list(filter(lambda job: job["action.start_time"] - job["repo.push.date"] <= JOB_START_DELAY_MAX, jobGroup["jobs"]))
    jobsNormalTime.sort(key=lambda job: job["action.start_time"])
    jobGroup["jobs"] = jobsNormalTime
    for job in jobsNormalTime:
        # 1 classification: string; 2+ classifications: list
        if isinstance(job["failure.notes.created"], list):
            classificationTime = min(job["failure.notes.created"])
        # only one classification time, float instead of list
        else:
        # RESPONSE_LIMIT threshold in which action must be taken
            classificationTime = job["failure.notes.created"]
        if max(0, int(classificationTime) - lastTimeOk) < CLASSIFICATION_DELAY_MAX:
            classificationTimedeltas.append(max(0, int(classificationTime) - lastTimeOk))
classificationTimedeltas.sort()
if DEBUG:
    print("Classification times for individual tasks (position: seconds)")
    for pos in range(len(classificationTimedeltas)):
        print("%(i)i: %(value).0f" %
            {
                'i': pos,
                'value': classificationTimedeltas[pos],
            })
classificationsToUse = int(round(PERCENTAGE_TO_USE / 100 * len(classificationTimedeltas)))
if len(classificationTimedeltas) > 0 and classificationsToUse == 0:
    classificationsToUse = 1
print("classifications used for calculation (count):", len(classificationTimedeltas))
print("average classification time (s):", int(round(sum(classificationTimedeltas[0:classificationsToUse]) / classificationsToUse)))
print("limit classification time (s):", classificationTimedeltas[classificationsToUse - 1])
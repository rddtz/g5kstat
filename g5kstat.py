#!/usr/bin/python

# TODO: add type hints
# TODO: add more options
# TODO: create a check free option

import os
import sys
import requests
import socket

from time import sleep
from tabulate import tabulate
import argparse
from time import time
from datetime import datetime

SITES=['grenoble', 'lille', 'luxembourg', 'louvain', 'lyon', 'nancy', 'nantes', 'rennes', 'sophia', 'strasbourg', 'toulouse']
WRONG_PARAMETER=501
API_SITES = "https://api.grid5000.fr/stable/sites/"

def ParseArgs():

    parser = argparse.ArgumentParser(prog="g5kstat", description='OAR stats because I didn\'t liked the oarstat.')
    parser.add_argument('-n', '--textmax', type=int, default=20, required=False, help="Max text width")
    parser.add_argument('-m', '--results', type=int, default=sys.maxsize, required=False, help="Number to lines in the output")
    parser.add_argument('-s', '--site', type=str.lower, default="", choices=SITES, required=False,  help='Site to query for')
    parser.add_argument('-f', '--free', required=False, action='store_true', help='Show free resources in the site')
    parser.add_argument('--dead', required=False, action='store_true', help='Show info about deda hosts also')


    return parser.parse_args()

def error(msg: str, code: int):

    if code == WRONG_PARAMETER:
        try:
            raise NameError(1)
        except NameError:
            print(f"[Error] {msg}")
            exit(code)

def parse_cores(resources, args):

    hosts = sorted([(x.split(".")[0], x.split("/")[1]) for x in resources])

    hosts_dict = {}

    for i in hosts:
        try:
            hosts_dict[i[0]].append(i[1])
        except KeyError:
            hosts_dict[i[0]] = [i[1]]

    ret = []
    for k, v in hosts_dict.items():
        cores = sorted(list(map(int, v)))
        start = cores[0]
        final = -1
        past = start
        final_string = ""
        full_seq = True
        for i in cores[1:]:
            if i != past + 1:
                final = past
                if final == start:
                    final_string += str(start) + ","
                else:
                    final_string += str(start) + "-" + str(final) + ","

                start = i
            past = i

        final = cores[-1]
        if final == start:
            final_string += str(start) + ","
        else:
            final_string += str(start) + "-" + str(final) + ","

        final_string = f"{k}[{final_string[:len(final_string)-1]}]"
        ret.append(final_string)

    ret = ", ".join(ret)
    if len(ret) > args.textmax:
        ret = ret[:args.textmax - 3] + "..."

    return ret

def get_cores(api_job_url, jid, args):

    ret = ""


    query = requests.get(api_job_url + "/" + str(jid), auth=g5k_auth).json()['resources_by_type']
    for k, v in query.items():
        if k == 'cores':
            ret = parse_cores(v, args)

    return ret

def get_time(time):

    days = int(time // 3600 // 24)
    remaining = time % (3600 * 24)

    hours = int(remaining // 3600)
    remaining = remaining % 3600

    minutes = int(remaining // 60)
    seconds = remaining % 60

    if days > 0:
        return f"{days}D+{hours:02d}:{minutes:02d}:{seconds:02.0f}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02.0f}"

# When querying the API from the frontends, users are already identified.
# However, if using this script from the outside of Grid'5000, g5k_auth
# must be a tuple of strings: (username, password)



def queue(args, site):
    api_job_url = API_SITES + f"{site}/jobs"
    parameters = "?state=waiting,lauching,running,hold"
    jobs = requests.get(api_job_url + parameters, auth=g5k_auth).json()

    table = [(job['uid'],
              job['user'],
              job.get('name', '')[:args.textmax],
              "not started" if job['started_at'] == 0 else get_time(time() - job['started_at']),
              get_time(job['walltime']),
              job['state'],
              get_cores(api_job_url, job['uid'], args)
                  )
             for job in jobs['items'][:max(0, args.results)]]

    print(tabulate(table, headers=["JOB ID", "USER", "NAME", "TIME SINCE START", "WALLTIME", "STATE", "NODES AND CORES"], tablefmt="orgtbl"))
    print(f"Total of {jobs['total']} jobs found (showing {min(max(0, args.results), jobs['total'])}).")


def free(args, site):
    api_site_status = API_SITES + f"{site}/status"
    nodes = requests.get(api_site_status, auth=g5k_auth).json()['nodes']

    table = sorted([(name.split(".")[0],
              values['hard'],
              values['soft'],
              values['busy_slots'],
              values['free_slots'],
              "YES" if len(values['reservations']) != 0 else "NO",
              values['comment'][:args.textmax]
              )
             for name, values in nodes.items() if (values['hard'] != "dead" or args.dead)])
    print(tabulate(table, headers=["NODE", "HARDSTATE", "SOFTSTATE", "BUSY SLOTS", "FREE SLOTS", "RESERVED?", "COMMENTS"], tablefmt="orgtbl"))

if __name__ == "__main__":
    g5k_auth = None

    args = ParseArgs()


    site = args.site
    if site not in SITES:
        site = socket.gethostname()
        print(f"[Warning] Site not provided through -s, searching based on current host ({site})...")
        if site[1:] not in SITES:
            error(f"Currently host ({site}) is not a valid site!", WRONG_PARAMETER)
        else:
            site = site[1:]

    args.site = "here"
    if args.free:
        free(args, site)
    else:
        queue(args, site)

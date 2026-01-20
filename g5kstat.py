#!/usr/bin/python

import os
import sys
import requests
from time import sleep
from tabulate import tabulate
import argparse
from time import time, strftime, gmtime
from datetime import datetime

def ParseArgs():

    sites=['grenoble', 'lille', 'luxembourg', 'louvain', 'lyon', 'nancy', 'nantes', 'rennes', 'sophia', 'strasbourg', 'toulouse']

    parser = argparse.ArgumentParser(description='OAR stats because I didn\'t liked the oarstat.')
    parser.add_argument('-n', '--textmax', type=int, default=20, required=False, help="Max text width")
    parser.add_argument('-m', '--results', type=int, default=sys.maxsize, required=False, help="Max text width")
    parser.add_argument('-s', '--site', type=str.lower, choices=sites, required=True,  help='Site to query for')


    return parser.parse_args()


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

    return ", ".join(ret)

def get_cores(jid, args):

    ret = 0


    ret = requests.get(api_job_url + "/" + str(jid), auth=g5k_auth).json()['resources_by_type']
    for k, v in ret.items():
        if k == 'cores':
            ret = parse_cores(v, args)

    return ret

# When querying the API from the frontends, users are already identified.
# However, if using this script from the outside of Grid'5000, g5k_auth
# must be a tuple of strings: (username, password)
g5k_auth = None

args = ParseArgs()


api_sites = "https://api.grid5000.fr/stable/sites/"
api_job_url = api_sites + f"{args.site}/jobs"
parameters = "?state=waiting,lauching,running,hold"
jobs = requests.get(api_job_url + parameters, auth=g5k_auth).json()

table = [(job['uid'],
          job['user'],
          job.get('name', '')[:args.textmax],
          strftime("%H:%M:%S", gmtime(time() - job['submitted_at'])),
          job['state'],
          get_cores(job['uid'], args)

          )
         for job in jobs['items'][:max(0, args.results)]]

print(tabulate(table, headers=["JOB ID", "USER", "NAME", "TIME UP", "STATE", "NODES AND CORES"], tablefmt="orgtbl"))
print(f"Total of {jobs['total']} jobs found (showing {min(max(0, args.results), jobs['total'])}).")

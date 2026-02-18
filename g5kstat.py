#!/usr/bin/python

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
    """
    Parse CLI commands
    """

    # TODO: add more options, create groups
    # add different arguments for the query machine, maybe using the query disponible in the G5K
    
    parser = argparse.ArgumentParser(prog="g5kstat", description='OAR stats because I didn\'t liked the oarstat.')
    parser.add_argument('-n', '--textmax', type=int, default=40, required=False, help="Max text width")
    parser.add_argument('-m', '--results', type=int, default=sys.maxsize, required=False, help="Number to lines in the output")
    parser.add_argument('-s', '--site', type=str.lower, default="", choices=SITES, required=False,  help='Site to query for')
    parser.add_argument('-f', '--free', required=False, action='store_true', help='Show free resources in the site')
    parser.add_argument('--dead', required=False, action='store_true', help='Show info about deda hosts also')
    parser.add_argument('-u', '--user', required=False, default="", help='User to query jobs from')

    return parser.parse_args()

def error(msg : str, code : int):
    """Show error and exists.

    Parameters
    ----------
    msg : str
        Error message.
    code : int
        Error code.

    """    
    
    if code == WRONG_PARAMETER:
        try:
            raise NameError(1)
        except NameError:
            print(f"[Error] {msg}")
            exit(code)
    else:
        print(f"[Error] Existing on code {code}")
        exit(code)    
            
def parse_cores(resources : list[str], textmax : int) -> str:
    """Get a list of resouces and return a string of type hosts and cores being used

    Parameters
    ----------
    resources : list[str]
        Names of the resourcecs being used, G5K sends the info as HOST/CORE
    textmax : int
        Maximum number of caracterd that can be used
    
    Returns
    -------
    str
        String like 'host[cores], host[cores]' with info about hosts and cores.

    Notes
    -----
    If the resources list pass the textmax size, the rest of the string will be replaced with '...':
    'host[cores], host[cores]' -> 'host[cores], hos...'
    """

    hosts = sorted([(x.split(".")[0], x.split("/")[1]) for x in resources])

    hosts_dict = {}

    for i in hosts:
        try:
            hosts_dict[i[0]].append(i[1])
        except KeyError:
            hosts_dict[i[0]] = [i[1]]
            
    ret = []
    for k, v in hosts_dict.items():
        # Create a list of cores like 0-10 if contiguous or 0,3,5 if not

        cores = sorted(list(map(int, v)))
        
        start = cores[0]
        final = -1
        
        past = start
        
        final_string = ""
        
        for i in cores[1:]: # check continuity
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
    if len(ret) > textmax:
        ret = ret[:textmax - 3] + "..."

    return ret

def get_cores(api_job_url : str, jid : int, textmax : int) -> str: 
    """Query Grid5000 for info about the resources of one job.

    Parameters
    ----------
    api_job_url : str
        Grid5000 API Url for jobs querying.
    jid : int
        Job ID.
    textmax : int
        Maximum number of caracterd that can be used.

    Returns
    -------
    str
        String like 'host[cores], host[cores]' with info about hosts and cores.

    Notes
    -----
    If the resources list pass the max_text size, the rest of the string will be replaced with '...':
    'host[cores], host[cores]' -> 'host[cores], hos...'
    """
   
    ret = ""

    query = requests.get(api_job_url + "/" + str(jid), auth=g5k_auth).json()['resources_by_type']
    for k, v in query.items():
        if k == 'cores':
            ret = parse_cores(v, textmax)

    return ret

def get_time(time : int) -> str:
    """Return a formated string about for the time passed in seconds.

    Parameters
    ----------
    time : int
        Time in seconds.


    Returns
    -------
    str
        String informing the ime passed in hours, minutes and seconds.

    Notes
    -----
    If the time surpasses one day, it will be informed bu sinalizing the number of days passed
    with D+, by example, if the Job is running for 36 hours, the output would be 1D+12:00:00
    """
   
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

def queue(site : str, user : str, results : int, textmax : int) -> None:
    """Prints information about the job queue for a determinated site.

    Parameters
    ----------
    site : str
        Grid5000 Site for query (ex.: Grenoble)
    user : str
        Grid5000 user for query jobs from
    results : int
        Number of results in the output, maximun number of jobs to query for.
    textmax : int
        Maximum number of caracterd that can be used.
    """

    api_job_url = API_SITES + f"{site}/jobs"

    user_query = ""
    if user != "":
        user_query = f"user={user}&"
    
    parameters = f"?{user_query}state=waiting,lauching,running,hold&limit={results}"
    jobs = requests.get(api_job_url + parameters, auth=g5k_auth).json()

    table = [(job['uid'],
              job['user'],
              job.get('name', '')[:textmax],
              "not started" if job['started_at'] == 0 else get_time(time() - job['started_at']),
              get_time(job['walltime']),
              job['state'],
              get_cores(api_job_url, job['uid'], textmax)
            )
            for job in jobs['items']]
    
    print(tabulate(table, headers=["JOB ID", "USER", "NAME", "TIME SINCE START", "WALLTIME", "STATE", "NODES AND CORES"], tablefmt="orgtbl"))
    print(f"Total of {jobs['total']} jobs found (showing {min(max(0, results), jobs['total'])}).")


def free(site: str, textmax : int, dead : bool) -> None:
    """Prints information about the machines available in a determinated site.

    Parameters
    ----------
    site : str
        Grid5000 Site for query (ex.: Grenoble)
    textmax : int
        Maximum number of caracterd that can be used.
    dead : bool
        Show dead machines
    """

    api_site_status = API_SITES + f"{site}/status"
    nodes = requests.get(api_site_status, auth=g5k_auth).json()['nodes']

    table = sorted([(name.split(".")[0],
              values['hard'],
              values['soft'],
              values['busy_slots'],
              values['free_slots'],
              "YES" if len(values['reservations']) != 0 else "NO",
              values['comment'][:textmax]
              )
             for name, values in nodes.items() if (values['hard'] != "dead" or dead)])
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

    if args.free:
        free(site, args.textmax, args.dead)
    else:
        queue(site, args.user, args.results, args.textmax)

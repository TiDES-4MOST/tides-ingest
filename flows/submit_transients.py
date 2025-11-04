#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This tool is an example of how to work with transients using a RESTful API
within the 4FS Web Interface. It provides both proof-of-concept python-based
routines for performing many actions, as well as rich commandline interface
for performing many actions from a terminal. The rest of this docstring does
describe many aspects of the tool, but you are implored to try the '--help'
input argument from the commandline yourself.

You'll see working examples for how to:
    - establish a connection to the API, using either your username+password or
      an access token
    - retrieve OPTIONS and a description of the API's schema
    - list known transients (with or without filters)
    - submit a new transient
    - update a specific transient
    - delete a specific transient

That is, the RESTful API provides a full CRUD service (i.e. Create, Retrieve,
Update, Delete) as covered by the points listed above, and this is possible
using a standard HTTP library and your preferred data-manipulation methods.

This tool uses the third-party "requests" package for making GETting/POSTing
requests from/to the 4FS_WI API. For more advanced usage, please see
https://requests.readthedocs.io/en/latest/user/quickstart/ and
https://requests.readthedocs.io/en/latest/user/advanced/.

The 4FS_WI transients API is based on the Django REST framework, and the URL
schema is based on https://www.django-rest-framework.org/api-guide/routers/#defaultrouter,
where {prefix} is the SCHEMA URL: https://4most.mpe.mpg.de/QFSwi/targetCat/transients/,
which is also defined below as an internal variable.

In general, use of the API requires a valid username+password as with the
standard use of the 4MOST 4FS Web Interface. Additionally, one may also use
an authentication token that allows access only to the transients API, which
may be preferred in lieu of hardcoding your secretive password. A routine and
CLI argument are available here for retrieving such aforementioned token.

Filters may be applied to the queryset, using an SQL-like grammar for evening
controlling the matching of a particular field against a value. Several examples
are presented here:

Any string-/value-based field may be filtered directly, just note that
all field names are defined using strictly lowercase (as opposed to the
input file requirements using uppercase!):
    {SCHEMA}/?{field}={value}
So, for example, any transients with the name matching "somefavoritename":
    {SCHEMA}/?name=somefavoritename

The filtering may also use suffixes as part of the name of the field to
describe the "look-up type" (note the double-underscore between the field
name and its suffix!):
    {SCHEMA}/?{field__lookuptype}={value}
Available lookuptype keywords are described under
https://docs.djangoproject.com/en/2.2/topics/db/queries/ and include:
    - "gt" and "lt" for greater-than and less-than
    - "gte" and "lte" for gt-or-equal and lt-or-equal
    - "startswith" or "istartswith" for checking against the beginning of a string
    - "endswith" or "iendswith" for checking against the ending of a string
    - "exact" or "iexact" for checking exactly (or) against a string
    - "contains" for checking within a string (or case-insensitive: "icontains")
    - "isnull" for looking for a missing value (if not empty or 0)
So, for example, one could collect all transients with non-zero date_latest:
    {SCHEMA}/?date_latest__iexact=0

One may also chain such filters but only using logical AND via the
ampersand character, '&'. For example, to find entries with zero-valued
date_latest and declination at or above 0°:
    {SCHEMA}/?date_latest=0&dec__gte=0
Currently, more advanced filtering is not available, however we could try
to implement something using another third-party plugin if needed, such as
this one: https://github.com/philipn/django-rest-framework-filters.

A note about the time zone-based fields: the date_submitted and date_modified
fields are automatically set during creation/modification. You can also
filter against them, but note that the local-to-Germany time zone is used
(i.e. UTC+1 or UTC+2). If you want to filter one of these fields without a
time zone, you can just use a value in a YYYY-MM-DD HH:MM[:ss[.uuuuuu]]
format. If you want to specify a time zone, you must add it at the end of
the date-time-stamp as "Z+XX" or "Z-XX" (or without a "+"). The requirement
of including "Z" is only relevant to URL-based API access and does not
affect the web-interface-based search form. Timestamps which do not include
a "Z" (and hence not time zone-aware) will automatically be matched with
the local time zone (Europe/Berlin), and timestamps which do include a
"Z" but without an offset will be assumed to be UTC.

As an example, if we consider three transients submitted at three times across
an afternoon:
    - "date_submitted": "2021-09-21T12:01:57.283596+02:00",
    - "date_submitted": "2021-09-21T14:01:57.283596+02:00",
    - "date_submitted": "2021-09-21T20:01:57.283596+02:00",
you could collect them all by querying any one of these (note that a "T"
is optional between the date and time):
    - {SCHEMA}/?date_submitted__gt=2021-09-21
    - {SCHEMA}/?date_submitted__gt=2021-09-21 12:00:00
    - {SCHEMA}/?date_submitted__gt=2021-09-21T12:00:00Z02,
    - {SCHEMA}/?date_submitted__gt=2021-09-21T12:00:00Z10,
but you wouldn't pick up the first one if you check against UTC:
    - {SCHEMA}/?date_submitted__gt=2021-09-21T12:00:00Z+00.

--

Copyright (c) 2021 Jacob Laas <jclaas@mpe.mpg.de>
Distributed under the MIT license:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
DEBUG = True
# standard library
import os
import sys
import logging, logging.handlers
import argparse
import time
from timeit import default_timer as timer
import distutils.version
import traceback as tb
import datetime
import pprint
import json
# third-party
import requests
from requests.auth import HTTPBasicAuth
# local
pass

# set up logging functionality
# NOTE: only logging to terminal!
logformat = '%(asctime)s - %(name)s:%(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logformat)
log = logging.getLogger("4fs-transients-%s" % os.getpid())
if DEBUG:
    log.setLevel(logging.DEBUG)
log.info("**** STARTING A NEW SESSION (PID: %s) ****" % os.getpid())

### define parameters and client
# SCHEMA = "http://127.0.0.1:8080/targetCat/transients/"
SCHEMA = "https://4most.mpe.mpg.de/QFSwi/targetCat/transients/"
USERNAME = None
PASSWORD = None
ACCESS_TOKEN = None
pp = pprint.PrettyPrinter(indent=4, compact=True)

### define an example payload and provide routine for usefully renaming it
DEFAULT_PAYLOAD = {
    "uploadedfor_survey_id": 1,
    "name": "NO_NAME",
    "ra": 0.0,
    "dec": 0.0,
    "pmra": 0.0,
    "pmdec": 0.0,
    "epoch": 2021,
    "resolution": 1,
    "subsurvey": "SUBSURVEY",
    "cadence": 0,
    "template": "TEMPLATE_NAME",
    "ruleset": "RULESET_NAME",
    "redshift_estimate": 0.0,
    "redshift_error": 0.0,
    "extent_flag": 0,
    "extent_parameter": 0,
    "extent_index": 0,
    "mag": 20,
    "mag_err": 0,
    "mag_type": "MAG_TYPE",
    "reddening": 0,
    "date_earliest": 0,
    "date_latest": 0,
    "t_exp_b": 0,
    "t_exp_d": 0,
    "t_exp_g": 0,
    "is_active": True,
}

### helper routines

def modified_payload(payload=None):
    """
    This is a helper routine for simply copying some default payload
    and renaming the target's name to be something useful for unique
    identification.

    Parameters
    ----------
    payload : dict, optional
        an alternative payload of data defining a new target

    Returns
    -------
    dict
        a payload of data
    """
    if payload is None:
        payload = dict(DEFAULT_PAYLOAD)
    else:
        payload = dict(payload)  # to avoid modifying the original
    # update with a new name
    payload.update({
        "name":datetime.datetime.now().strftime(format="%Y%m%d %H%M%S")
    })
    # return it
    return payload

def get_session(username=USERNAME, password=PASSWORD, token=ACCESS_TOKEN):
    """
    This routine simply provides a session for the new request, which
    makes it easier to systematically update the authentication credentials.

    Parameters
    ----------
    username : str, optional
        the username to use
    password : str, optional
        the password to use
    token : str, optional
        the access token to use

    Returns
    -------
    requests.Session
        the session for the new request
    """
    s = requests.Session()
    if (username is not None) and (password is not None):
        s.auth = (username, password)
    if (token is not None):
        s.headers.update({"Authorization": "Token %s" % (token,)})
    return s

def check_request(request=None, caller="(caller N/A)", printout=False):
    if request is None:
        return
    try:
        if request.status_code in (requests.codes.ok, 204):
            msg = f"{caller} ran successfully"
            try:
                results = json.loads(request.text)
                if "err" in results:
                    raise Exception(f"retrieved an ERROR: {results['err']}")
                if printout:
                    msg += ": %s" % (results)
                log.info(msg)
                return results
            except UserWarning:
                if printout:
                    msg += ": %s" % (request.text)
        else:
            raise Exception(f"{caller} failed with HTTP code {request.status_code}! Response was: {request.text}")
    except Exception:
        e = sys.exc_info()
        log.warning(f"{e[1]}")
        return request
    log.info(msg)
    return request.text


### main routines

def get_api_token(printout=True):
    """
    Returns the queried authentication token for the transients API.

    Note that this is currently the only way to collect it (short of
    contacting the 4FS_WI administrator directly).

    Returns
    -------
    str or requests.Response
        either a) normally the queried API token as a string, or b) the HTTP response in case of errors
    
    Notes
    -----
    In case of an error, the full requests.Response is returned. In that case,
    one should reference https://docs.python-requests.org/en/latest/api/ for
    how to proceed. Typically the error message will be the first set of lines
    within r.text, and the r.status_code will not be 2XX but more like a value
    or 4- or 5-hundred-something. A full list of possbile HTTP response codes
    is available here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    url = url.replace("targetCat/transients", "transients-token-auth")
    session = get_session(username=username, password=password, token=token)
    r = session.post(
        url,
        data={"username": username, "password": password},
    )
    return check_request(request=r, caller="get_api_token()", printout=printout)

def get_options(printout=True):
    """
    Simply prints (optional) and returns the list of OPTIONS describing
    the API schema.

    Parameters
    ----------
    printout : bool, optional, default=True
        whether to print out the OPTIONS in addition to returning the text

    Returns
    -------
    str
        the results from the OPTIONS query
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    session = get_session(username=username, password=password, token=token)
    r = session.options(
        url,
    )
    results = check_request(request=r, caller="get_options()", printout=printout)
    pp.pprint(results)
    return results

def get_list(pk=None, flt=None):
    """
    Returns a queried list of transients.

    Note that the list contains ALL the elements available, within the user's
    survey membership, however, additional filters may be applied to select
    any particular subset.

    Parameters
    ----------
    pk : int, optional, default=None
        the id of an individual transient (if not a set)
    flt : str, optional, default=None
        a (set of) field+lookup pair(s) for applying selection filters

    Returns
    -------
    str or requests.Response
        either a) normally the queried transient (or set thereof) in format JSON, or b) the HTTP response in case of errors
    
    Notes
    -----
    In case of an error, the full requests.Response is returned. In that case,
    one should reference https://docs.python-requests.org/en/latest/api/ for
    how to proceed. Typically the error message will be the first set of lines
    within r.text, and the r.status_code will not be 2XX but more like a value
    or 4- or 5-hundred-something. A full list of possbile HTTP response codes
    is available here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    if pk is not None:
        url = "%s/%s/" % (url.rstrip("/"), pk)
    if flt is not None:
        url = "%s/?%s" % (url.rstrip("/"), flt)
    session = get_session(username=username, password=password, token=token)
    r = session.get(
        url,
    )
    return check_request(request=r, caller="get_list()", printout=False)

def show_list(*args, **kwargs):
    """
    Prints out the queried list of transients. Note that this simply calls
    get_list() with the same full set of input arguments, and then prints
    out to the terminal.
    """
    retrieved_list = get_list(*args, **kwargs)
    try:
        pp.pprint(json.loads(retrieved_list))
    except Exception:
        print(retrieved_list)

def create_transient(data=None, printout=True):
    """
    Submits a new transient.

    Note that by default, a payload containing mostly zeroes is used to describe
    the new transient, defined by the dictionary DEFAULT_PAYLOAD. Alternatively,
    you can provide your own dictionary (or list thereof) describing the new
    transient(s).

    Parameters
    ----------
    data : dict or list, optional, default=None
        the dict (or list thereof) describing the new transient
    printout : bool, optional, default=True
        whether to print out to the terminal the post-submission HTTP response

    Returns
    -------
    str or requests.Response
        the post-submission HTTP response
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    if data is None:
        data = modified_payload()
    elif isinstance(data, (dict, list)):
        pass
    else:
        payload = dict(DEFAULT_PAYLOAD)
        data = json.loads(data)
        payload.update(data)
        data = payload
        del payload
    data = json.dumps(data)
    session = get_session(username=username, password=password, token=token)
    r = session.post(
        url,
        json=data
    )
    return check_request(request=r, caller="create_transient()", printout=printout)

def update_transient(pk=None,
                     data=None,
                     printout=True):
    """
    Updates a current transient with new data. Note that this routine uses
    strictly the PATCH method to provide a partial update to a transient,
    which means you can provide all the new fields or simply a subset thereof.

    Parameters
    ----------
    pk : int
        the id of an individual transient
    data : dict
        the dict describing the data to use during the update
    printout : bool, optional, default=True
        whether to print out to the terminal the post-submission HTTP response

    Returns
    -------
    str or requests.Response
        the post-submission HTTP response
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    if pk is None:
        return
    else:
        url = "%s/%s/" % (url.rstrip("/"), pk)
    if data is None:
        data = modified_payload()
    elif isinstance(data, dict):
        pass
    else:
        data = json.loads(data)
    session = get_session(username=username, password=password, token=token)
    r = session.patch(
        url,
        json=data
    )
    return check_request(request=r, caller="update_transient()", printout=printout)

def delete_transient(pk=None):
    """
    Deletes a current transient.

    Note that this is NOT ALLOWED if the transient has already been ingested by
    OpSys into their OSTD (OpSys Target Database). If the transient has already
    been ingested, it is recommended to flag the transient from "is_active=True"
    to "is_active=False".

    Parameters
    ----------
    pk : int
        the id of an individual transient

    Returns
    -------
    str or requests.Response
        the post-submission HTTP response
    """
    url = SCHEMA
    username = USERNAME
    password = PASSWORD
    token = ACCESS_TOKEN
    if pk is None:
        return
    else:
        url = "%s/%s/" % (url.rstrip("/"), pk)
    session = get_session(username=username, password=password, token=token)
    r = session.delete(
        url,
    )
    return check_request(request=r, caller="delete_transient()", printout=printout)

### test routine(s)

def test_multi_simul():
    """
    Bulds and submits TWO transients at once.
    """
    # build payload
    multiple_transients = []
    multiple_transients.append(modified_payload())
    multiple_transients.append(modified_payload())
    log.debug("payload: %s" % (multiple_transients,))
    # submit all at once
    timer_submission_start = timer()
    create_transient(data=multiple_transients)
    timer_submission_stop = timer()
    time_to_submit = timer_submission_stop - timer_submission_start
    # try to catch them
    time_now = datetime.datetime.now()
    time_submission = time_now - datetime.timedelta(seconds=30)
    time_submission_str = time_submission.strftime("%Y-%m-%dT%H:%M:%S")
    log.debug("time_submission_str: %s" % (time_submission_str,))
    show_list(flt="date_submitted__gt=%s" % (time_submission_str,))

def run_test():
    """
    This routine just provides code which can be quickly edited
    for testing something in the same tool.

    It may be activated with the "--runtest" command-line argument.
    """
    test_multi_simul()



if __name__ == '__main__':
    """Set up the argument parser for CLI usage."""

    ### define input arguments
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--debug", action='store_true',
        help="whether to add extra print messages to the terminal"
    )
    # schema/auth items
    parser.add_argument(
        "--schema", type=str, default=None,
        help="overrides the URL schema for the API"
    )
    parser.add_argument(
        "--username", type=str, default=None,
        help="sets a username for the API"
    )
    parser.add_argument(
        "--password", type=str, default=None,
        help="sets a password for the API"
    )
    parser.add_argument(
        "--token", type=str, default=None,
        help="sets an access token for the API"
    )
    # control element details
    parser.add_argument(
        "--id", type=int, nargs="?",
        help="the ID of the transient"
    )
    parser.add_argument(
        "--data", type=str, default=None,
        help="allows you provide JSON-like data for overriding the default payload"
    )
    parser.add_argument(
        "--filter", type=str, default=None,
        help="sets a filter (field+lookuptype) to the URL schema (list-only!)"
    )
    # standard actions
    parser.add_argument(
        "--request-token", action='store_true',
        help="requests the auth token for the API (NOTE: requires a valid user+password combination)"
    )
    parser.add_argument(
        "--options", action='store_true',
        help="simply returns the OPTIONS describing the schema"
    )
    parser.add_argument(
        "--list", action='store_true',
        help="simply returns the full list of transients"
    )
    parser.add_argument(
        "--create", action='store_true',
        help="creates a new transient"
    )
    parser.add_argument(
        "--update", type=int, default=None,
        help="updates the third transient with a name matching the current timestamp"
    )
    parser.add_argument(
        "--delete", type=int, default=None,
        help="updates the third transient with a name matching the current timestamp"
    )
    # dev-related controls
    parser.add_argument(
        "--usetutorialpayload", action='store_true',
        help="(dev only) fixes the transient payload to be compatible with the 4FS_WI Tutorial files"
    )
    parser.add_argument(
        "--runtest", action='store_true',
        help="(dev only) launch the run_test() routine instead of the standard routines"
    )

    ### parse arguments
    args = parser.parse_args()
    if args.debug and not DEBUG:
        log.info("activating debugging mode via CLI")
        log.setLevel(logging.DEBUG)
        DEBUG = True
    if DEBUG:
        log.info("args were: %s" % (args,))
    if args.schema is not None:
        SCHEMA = args.schema
    if args.username is not None:
        USERNAME = args.username
    if args.password is not None:
        PASSWORD = args.password
    if args.token is not None:
        ACCESS_TOKEN = args.token
    
    ### override payload if desired
    if args.usetutorialpayload:
        DEFAULT_PAYLOAD.update({
            "ra": 165.46627262,
            "dec": -34.70473099,
            "subsurvey": "sub1",
            "template": "s4250:g+1.5:m1.0:t02:z-0.50:a+0.00.AMBRE:ebv0.0.fits",
            "ruleset": "HR_BLUE",
            "mag_type": "Johnson_V_Vega",
            "resolution": 2,
            "t_exp_b": 20,
            "t_exp_d": 20,
            "t_exp_g": 20,
        })
    
    ### launch one of the main routines after everything else is complete
    if args.runtest:
        run_test()
    if args.request_token:
        get_api_token()
    elif args.options:
        get_options()
    elif args.list:
        show_list(pk=args.id, flt=args.filter)
    elif args.create:
        create_transient(data=args.data)
    elif args.update:
        update_transient(pk=args.update, data=args.data)
    elif args.delete:
        delete_transient(pk=args.delete)

    sys.exit()

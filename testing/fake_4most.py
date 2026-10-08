"""
fake_4most.py

An in-memory stand-in for the 4MOST transients API, for tests.

    fake = fake_4most.install()

After install(), EVERY request made through submit_transients (POST/PATCH/GET/DELETE, whatever
st.URL_SCHEMA says) is answered by this fake, so a test can never reach the real 4MOST.

fake.fail_mode:
    None             behave like a healthy API (201 on create, 200 on patch/get)
    'down'           every request raises ConnectionError (nothing is created)
    'lose_response'  POSTs are CREATED, then the response is lost (ReadTimeout) - the case
                     that used to produce duplicates on the next run
"""

import json
import os
import sys
from urllib.parse import parse_qs, urlsplit

import requests

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tides_flows'))
import submit_transients as st

FAKE_URL = "http://fake4most.local/targetCat/transients/"


def _response(request, status, body=None):
    r = requests.Response()
    r.status_code = status
    r._content = b'' if body is None else json.dumps(body).encode()
    r.headers['Content-Type'] = 'application/json'
    r.url, r.request = request.url, request
    return r


class Fake4MOST(requests.adapters.BaseAdapter):
    def __init__(self):
        super().__init__()
        self.targets = {}           # id -> target dict
        self.next_id = 1
        self.fail_mode = None
        self.calls = []             # (method, url) of every request seen

    def send(self, request, **kwargs):
        self.calls.append((request.method, request.url))
        if self.fail_mode == 'down':
            raise requests.exceptions.ConnectionError("fake 4MOST is down")
        url = urlsplit(request.url)
        parts = [p for p in url.path.split('/') if p]
        pk = int(parts[-1]) if parts and parts[-1].isdigit() else None
        body = json.loads(request.body) if request.body else None

        if request.method == 'POST':
            items = body if isinstance(body, list) else [body]
            created = []
            for item in items:
                target = dict(item, id=self.next_id)
                self.targets[self.next_id] = target
                self.next_id += 1
                created.append(target)
            if self.fail_mode == 'lose_response':
                raise requests.exceptions.ReadTimeout("fake: created, but the response was lost")
            return _response(request, 201, created if isinstance(body, list) else created[0])
        if request.method == 'PATCH':
            if pk not in self.targets:
                return _response(request, 404, {"detail": "Not found."})
            self.targets[pk].update(body)
            return _response(request, 200, self.targets[pk])
        if request.method == 'GET':
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            found = [t for t in self.targets.values()
                     if all(str(t.get(k)) == v for k, v in query.items() if k in ('name', 'subsurvey'))]
            return _response(request, 200, {"count": len(found), "next": None, "results": found})
        if request.method == 'DELETE':
            self.targets.pop(pk, None)
            return _response(request, 204)
        return _response(request, 405, {"detail": "method not allowed"})

    def close(self):
        pass


def install():
    """Route all submit_transients traffic to a new Fake4MOST and switch st writes on."""
    fake = Fake4MOST()

    def fake_session(username=None, password=None, token=None):
        session = requests.Session()
        session.mount('http://', fake)
        session.mount('https://', fake)     # even the production URL ends up here
        return session

    st.get_session = fake_session
    st.URL_SCHEMA = FAKE_URL
    st.WRITES_ALLOWED = True
    return fake

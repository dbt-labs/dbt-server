import requests
import time
import sys

UNPARSED_MANIFEST = 'https://gist.githubusercontent.com/drewbanin/05eccef8c018e471e728f5b265f93476/raw/31fe5bae30f1e42c86889dd891221da7415fd710/idk.json'

req = requests.get(UNPARSED_MANIFEST)
body = req.json()
print("GET UNPARSED", req.status_code) 

# POST /push

SERVER_HOST_PUSH    = 'http://127.0.0.1:8585/push'
SERVER_HOST_PARSE   = 'http://127.0.0.1:8585/parse'
SERVER_HOST_COMPILE = 'http://127.0.0.1:8585/compile'
SERVER_HOST_MEM     = 'http://127.0.0.1:8585/'

if len(sys.argv) > 1:
    loops = int(sys.argv[1])
else:
    loops = 10

for i in range(loops):
    state_id = f'debug-{i}'
    print(f"using state {state_id} (out of {loops})")

    # POST /push
    req = requests.post(SERVER_HOST_PUSH, json={"state_id": state_id, "body": body})
    print("PUSH", req.status_code)

    # POST /parse
    req = requests.post(SERVER_HOST_PARSE, json={"state_id": state_id})
    print("PARSE", req.status_code)

    req = requests.post(SERVER_HOST_COMPILE, json={
        "state_id": state_id,
        "sql": "select {{ not_a.valid_macro() }}"
    })
    print("COMPILE", req.status_code)

    req = requests.get(SERVER_HOST_MEM)
    print("MEMORY", req.content)
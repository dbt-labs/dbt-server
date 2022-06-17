### Config & setup

**Configure your machine**

1. Make sure you have a `~/.dbt/profiles.yml` file with a profile called "debug"
2. Create a virtualenv
3. Install dependencies in the virtualenv

```console
pip install -r requirements.txt
```

**Run the server**

From the root of the repo, with the virtualenv sourced:

```console
uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

### Workspace Mode
`WORKSPACE_MODE` overrides the signal handling for `SIGINT` and `SIGTERM` to allow the server to continue accepting requests after receiving either signal. A second signal will terminate the server normally.
`WORKSPACE_MODE` also enables the `POST /workspace-shutdown` endpoint, which is intended to be called by the Workspace Controller in a Workspace pod to forcefully shutdown the server, when there is no more work to process.

**NOTE: `WORKSPACE_MODE` is the default mode that dbt-server will run in, since it is how it should run in deployed environments.**

**NOTE: The signal handling overrides do not work properly with the `--reload` flag, so be sure to remove it to test or iterate on this functionality (everything else works fine and the server will restart as expected).**

To run without `WORKSPACE_MODE`, set the env var `WORKSPACE_MODE=off`:
```console
WORKSPACE_MODE=off uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

### Building docker container locally
```console
docker build -f Dockerfile . -t dbt-server-<dbt-core-version>:latest --build-arg DBT_CORE_VERSION=<dbt-core-version> --build-arg DBT_DATABASE_ADAPTER_PACKAGE=dbt-<database-adapter>
```

Example for building container for dbt-core v0.21.0 with the Snowflake database adapter:
```console
docker build -f Dockerfile . -t dbt-server-0.21.0:latest --build-arg DBT_CORE_VERSION=0.21.0 --build-arg DBT_DATABASE_ADAPTER_PACKAGE=dbt-snowflake
```

### Testing

Huh, it worked on my machine?

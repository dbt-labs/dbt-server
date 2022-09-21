# dbt-server


## 👩‍💻 Set up your development environment

### 🐍 Install Python

It is recommended to use [`pyenv`](https://github.com/pyenv/pyenv) to manage versions of Python you have installed on your machine. Follow [their installation](https://github.com/pyenv/pyenv#installation) instructions to get set up. If you are on MacOS, we recommend using the [`homebrew`](https://brew.sh) installation instructions.

Today, we do not standardize the Python versions for development environments. This will likely change in the future. For now, use `pyenv` to install the latest, long-term stable version of Python. For instance: `pyenv install 3.10.5`.
Then, instruct `pyenv` to use that version of Python every time you work on this project: `pyenv local 3.10.5`.
This will create a `.python-version` file in this directory. It is git-ignored, so it's okay if it is unique to you.
Now, every time you `cd` into this directory, the specified version of Python will be activated.

### 🤖 Setup a virtual environment

If you are not familiar with Python virtual environments, you may find [this primer from Real Python](https://realpython.com/python-virtual-environments-a-primer/) helpful.
There are several approaches to managing virtual environments. If you are new to virtual environments, then you can't go wrong with managing your environment directly as you get familiar with the concepts.

First, create a virtual environment called "venv" in the root of this repository

```bash
python -m venv venv
```

Now, every time you work on this project, you should activate the virtual environment. That way, you are using the version of Python and the packages that are installed for this project only, instead of your global Python installation.

```bash
source venv/bin/activate
```

### 📌 Install dependencies

#### Install requirements

For development, you need to install production and dev dependencies.

```bash
pip install -r requirements.txt -r dev-requirements.txt
```

#### Install dbt and adapter

Additionally, `dbt-server` expects that `dbt` is available in the Python environment. You can install dbt by first picking the adapter you want to work with and installing that. `dbt-core` will be installed alongside the adapter automatically.

If you don't have a particular database you want to work with, Postgres is a good choice. The adapter is well-used, has first-party support, and you can run Postgres locally.

```bash
pip install dbt-postgres # This installs dbt-core as well
```

More information on installing `dbt` can be found [here](https://docs.getdbt.com/dbt-cli/install/overview).

You will also need Postgres installed on your machine in order to use `dbt-postgres` locally. There a number of ways to install / run Postgres on your machine (homebrew, asdf, docker). If you don't have a preference, you can pick your platform and download from the [official download page](https://www.postgresql.org/download/).

### ⚙️ Create a dbt project for dbt-server to reference

`dbt-server` expects for a client to "push" a project to it, so to manually interact with `dbt-server`, you need a dbt project on your machine.
If you don't have a particular project that you want to work with, we recommend using [dbt-labs/jaffle_shop_metrics](https://github.com/dbt-labs/jaffle_shop_metrics) as an approachable sample project.

Make sure that you set up / clone the dbt project _outside_ of this repository. The dbt project should be on your machine, but in a different directory than dbt-server.

```bash
# From somewhere on your machine that you want this project to live. Perhaps ~/repos.
git clone https://github.com/dbt-labs/jaffle_shop_metrics.git
```

### 👩‍🔬 Run tests

This project uses `pytest` for testing, which is installed with your development dependencies.
To run all tests, simply run `pytest` in the root directory of this project.

```bash
pytest
```

### 💻 Run the server in development mode

From the root of this repository, make sure your virutal environment is activated.

```bash
uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

This will start the [Fast API](https://fastapi.tiangolo.com) server in development mode, with hot-code reloading.

## ✅ Build and run for production

### ⚡️ ALLOW_ORCHESTRATED_SHUTDOWN

`ALLOW_ORCHESTRATED_SHUTDOWN` overrides the signal handling for `SIGINT` and `SIGTERM` to allow the server to continue accepting requests after receiving either signal. A second signal will terminate the server normally.
`ALLOW_ORCHESTRATED_SHUTDOWN` also enables the `POST /shutdown` endpoint, which can be called by another application that controls the lifetime of dbt server.

**NOTE: The signal handling overrides do not work properly with the `--reload` flag, so be sure to remove it to test or iterate on this functionality (everything else works fine and the server will restart as expected).**

To run with `ALLOW_ORCHESTRATED_SHUTDOWN`, set the env var `ALLOW_ORCHESTRATED_SHUTDOWN=on`:

```bash
ALLOW_ORCHESTRATED_SHUTDOWN=on uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

### 🐳 Building docker container locally

```bash
docker build -f Dockerfile . -t dbt-server-<dbt-core-version>:latest --build-arg DBT_CORE_VERSION=<dbt-core-version> --build-arg DBT_DATABASE_ADAPTER_PACKAGE=dbt-<database-adapter>
```

Example for building container for dbt-core v0.21.0 with the Snowflake database adapter:

```bash
docker build -f Dockerfile . -t dbt-server-0.21.0:latest --build-arg DBT_CORE_VERSION=0.21.0 --build-arg DBT_DATABASE_ADAPTER_PACKAGE=dbt-snowflake
```

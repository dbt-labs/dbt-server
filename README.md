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

### ⚙️ Create a dbt project for dbt-server to reference

`dbt-server` expects for their to be a dbt project inside of `./working-dir`. This is the project that `dbt-server` allows clients to remotely interact with.

If you aren't sure where to start with this, we recommend that you clone the [`jaffle_shop`](https://github.com/dbt-labs/jaffle_shop) example dbt project into `working-dir`.

```bash
git clone https://github.com/dbt-labs/jaffle_shop.git ./working-dir
```

In order for your machine to run this `dbt` project, you need to configure a `~/.dbt/profiles.yml`. Make sure to set

### 📌 Install dependencies

For development, you need to install production and dev dependencies.

```bash
pip install -r requirements.txt -r dev-requirements.txt
```

Additionally, `dbt-server` expects that `dbt` is available in the Python environment.

It is easy to work with Postgres locally. So, with your virtual environment activated, install `dbt-postgres`, which will install `dbt-core` along with it.

```bash
pip install dbt-postgres
```

More information on installing `dbt` can be found [here](https://docs.getdbt.com/dbt-cli/install/overview).

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

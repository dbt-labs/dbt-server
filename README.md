### Config & setup

**Configure your machine**

1. Make sure you have a `~/.dbt/profiles.yml` file with a profile called "debug"
2. Create a virtualenv
3. Install dependencies in the virtualenv

```
pip install -r requirements.txt
```

**Run the server**

From the root of the repo, with the virtualenv sourced:

```
uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

### Testing

Huh, it worked on my machine?

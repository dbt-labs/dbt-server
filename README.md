
### Config & setup

**Configure your machine**

1. Make sure you have a `~/.profiles.yml` file with a profile called "debug"
2. Install dependencies 

```
poetry install
```


**Run the server**

From the root of the repo:

```
# Source the poetry env before kicking off the server
poetry shell
uvicorn dbt_server.server:app --reload --host=127.0.0.1 --port 8580
```

### Testing

Huh, it worked on my machine?

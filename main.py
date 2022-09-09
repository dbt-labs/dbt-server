import gunicorn
import requests

if __name__ == "__main__":
    gunicorn.run("dbt_server.server:app", host="127.0.0.1", port=8585, reload=False)

    # in thread keep calling /ready and when that returns 400 restart gunicorn
    should_restart = requests.get("/ready").status_code != 200


    # if should_restart:
    #     gunicorn.Server.shutdown()
    #     gunicorn.run("dbt_server.server:app", host="127.0.0.1", port=8585, reload=False)
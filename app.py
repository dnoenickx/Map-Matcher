from os import getenv
from pathlib import Path
from requests import Request, post
from threading import Thread

from dotenv import load_dotenv
from flask import Flask, request, url_for, redirect

import match

load_dotenv()

CLIENT_ID = getenv("CLIENT_ID")
CLIENT_SECRET = getenv("CLIENT_SECRET")
DIR = Path(__file__).parent.absolute()

app = Flask(__name__)


@app.route("/")
def start():
    external_url = (
        Request(
            "GET",
            "http://www.strava.com/oauth/authorize",
            params={
                "client_id": CLIENT_ID,
                "response_type": "code",
                "redirect_uri": url_for("exchange_token", _external=True),
                "approval_prompt": "force",
                "scope": "read,activity:read_all",
            },
        )
        .prepare()
        .url
    )

    return redirect(external_url)


@app.route("/exchange_token")
def exchange_token():
    res = post(
        "https://www.strava.com/oauth/token",
        params={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": request.args.get("code"),
            "grant_type": "authorization_code",
        },
    )
    if res.status_code != 200:
        return redirect("/")

    access_token = res.json()["access_token"]
    print(f"Access Token: {access_token}")

    Thread(target=match.run, args=(access_token,)).start()

    return f"Done. (Access Token: {access_token})"


if __name__ == "__main__":
    app.run()

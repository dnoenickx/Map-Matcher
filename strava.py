from requests import Request, post
import dotenv
from flask import Flask, request, url_for, redirect

DOTENV_FILE = dotenv.find_dotenv()
CLIENT_ID = dotenv.get_key(DOTENV_FILE, 'CLIENT_ID')
CLIENT_SECRET = dotenv.get_key(DOTENV_FILE, 'CLIENT_SECRET')

assert CLIENT_ID is not None and CLIENT_SECRET is not None
# 1) create a Strava API Application (https://www.strava.com/settings/api)
# 2) set CLIENT_ID and CLIENT_SECRET in .env file

app = Flask(__name__)

@app.route("/")
def authorize():
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
    return get_token(code=request.args.get("code"))


def get_token(code=None):
    params = {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    refresh_token = dotenv.get_key(DOTENV_FILE, 'REFRESH_TOKEN')

    if refresh_token is not None:
        params.update({"grant_type": "refresh_token", "refresh_token": refresh_token})
    else:
        params.update({"grant_type": "authorization_code", "code": code})


    res = post("https://www.strava.com/oauth/token", params=params)
    
    if res.status_code != 200:
        return redirect("/")

    dotenv.set_key(DOTENV_FILE, "ACCESS_TOKEN", res.json()["access_token"])
    dotenv.set_key(DOTENV_FILE, "REFRESH_TOKEN", res.json()["refresh_token"])

    return "Logged in."


def access_token():
    token = dotenv.get_key(DOTENV_FILE, 'ACCESS_TOKEN')
    assert token is not None
    return token

if __name__ == "__main__":
    app.run()

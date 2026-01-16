#!/usr/bin/env python3

import os
import sys
import logging
import slack
import requests
import json
import arrow
import asyncio
import aioredis
import pickle

from requests.auth import HTTPBasicAuth
from flask import Flask, jsonify, request
from slackeventsapi import SlackEventAdapter

ANCHORE_CLI_USER = os.environ.get("ANCHORE_CLI_USER", "admin")
ANCHORE_CLI_PASS = os.environ.get("ANCHORE_CLI_PASS")
ANCHORE_CLI_URL = os.environ.get("ANCHORE_CLI_URL", "http://hooky-anchore-engine-api:8228/v1")

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")

app = Flask(__name__)

gunicorn_logger = logging.getLogger("gunicorn.error")
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)

slack_events_adapter = SlackEventAdapter(SLACK_SIGNING_SECRET, "/slack/events", app)

client = slack.WebClient(token=os.environ["SLACK_API_TOKEN"])

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis-master")
REDIS_PASS = os.environ.get("REDIS_PASS")
REDIS_STREAM = "ae"

def get_emoji(data):
    if data:
        emoji = ":heavy_check_mark:"
    else:
        emoji = ":x:"
    return emoji

@slack_events_adapter.on("message")
def reaction_added(event_data):
    token = event_data["token"]
    team_id = event_data["team_id"]
    api_app_id = event_data["api_app_id"]
    event = event_data["event"]

@app.route("/healthz", methods=["GET"])
def healthz():
    return "OK"

# Slack webhook action handler
@app.route("/", methods=["POST"])
def index():
    data = json.loads(request.form.get("payload"))
    channel_id = data["container"]["channel_id"]
    user_id = data["user"]["id"]
    # response_url = data["response_url"]
    # actions = data["actions"]
    # repository = actions[0]["placeholder"]["text"]
    # tag = actions[0]["selected_option"]["text"]["text"]
    asyncio.run(redis_pool(data))
    response = client.chat_postEphemeral(
        channel=channel_id,
        user=user_id,
        text="Not Implemented."
    )
    return("", 204)

@app.route("/analyse_image", methods=["POST"])
def analyse_image():
    token = request.form.get("token", None)
    command = request.form.get("command", None)
    text = request.form.get("text", None)

    if not token:
        abort(400)

    response = client.chat_postMessage(
        channel=request.form.get("user_id", None),
        text="Hello world!")
    assert response["ok"]
    assert response["message"]["text"] == "Hello world!"
    return("", 204)

@app.route("/image_vulnerabilities", methods=["POST"])
def image_vulnerabilities():
    token = request.form.get("token", None)
    command = request.form.get("command", None)
    text = request.form.get("text", None)

    high = []
    medium = []
    low = []
    negligible = []
    unknown = []

    if not token:
        abort(400)

    try:
        repository = text.replace("<", "").replace(">", "").split("|")[1]
    except Exception as ex:
        repository = text

    image = _get_image_by_tag(repository)
    vulnerabilities = _get_image_vulnerabilities(image["image_detail"][0]["imageId"])

    for vulnerability in vulnerabilities["vulnerabilities"]:
        if vulnerability["severity"] == "High":
            high.append(vulnerability)
        elif vulnerability["severity"] == "Medium":
            medium.append(vulnerability)
        elif vulnerability["severity"] == "Low":
            low.append(vulnerability)
        elif vulnerability["severity"] == "Negligible":
            negligible.append(vulnerability)
        elif vulnerability["severity"] == "Unknown":
            unknown.append(vulnerability)

    slack_notification = {}
    slack_notification["blocks"] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Vulnerability Report*"
            }
        }
    ]

    options = []
    for vulnerability in high[:100]:
        options.append({
            "text": {
                "type": "plain_text",
                "text": f"{vulnerability['vuln']}",
                "emoji": True
            },
            "value": f"{vulnerability['url']}"
        })


    slack_notification["blocks"].append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "High Vulnerabilities:"
        },
        "accessory": {
            "type": "static_select",
            "placeholder": {
                "type": "plain_text",
                "text": f"Select a CVE",
                "emoji": True
            },
            "options": options
        }
    })

    # slack_notification["blocks"].append({"type": "divider"})
    # slack_notification["blocks"].append({
    #         "type": "context",
    #         "elements": [
    #             {
    #                 "type": "mrkdwn",
    #                 "text": f"*User*: {ae['userId']}\n*Subscription ID*: {ae['subscription_id']}"
    #             }
    #         ]
    #     }
    # )

    response = client.chat_postMessage(
        channel=SLACK_CHANNEL,
        blocks=slack_notification["blocks"]
    )
    return("", 204)

@app.route("/add_repository", methods=["POST"])
def add_repository():
    token = request.form.get("token", None)
    command = request.form.get("command", None)
    text = request.form.get("text", None)

    if not token:
        abort(400)

    try:
        repository = text.replace("<", "").replace(">", "").split("|")[1]
    except Exception as ex:
        repository = text

    ae = _add_repository(repository)[0]

    subscription_value = json.loads(ae["subscription_value"])

    slack_notification = {}
    slack_notification["blocks"] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Added Repository*"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Created At*:\n{arrow.Arrow.fromtimestamp(ae['created_at'])}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Last Updated*:\n{arrow.Arrow.fromtimestamp(ae['last_updated'])}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Subscription Key*:\n{ae['subscription_key']}"
                }
            ]
        }
    ]

    if subscription_value["repotags"]:
        options = []
        for tag in subscription_value["repotags"]:
            options.append({
                "text": {
                    "type": "plain_text",
                    "text": f"{tag}",
                    "emoji": True
                },
                "value": f"{tag}"
            })

        slack_notification["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Discovered Tags:"
            },
            "accessory": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": f"{repository}",
                    "emoji": True
                },
                "options": options
            }
        })

    slack_notification["blocks"].append({"type": "divider"})
    slack_notification["blocks"].append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*User*: {ae['userId']}\n*Subscription ID*: {ae['subscription_id']}"
                }
            ]
        }
    )

    response = client.chat_postMessage(
        channel=SLACK_CHANNEL,
        blocks=slack_notification["blocks"]
    )
    return("", 204)

@app.route("/general", methods=["POST"])
def general():
    """
    Anchore Engine General Update Slack notification webhook handler.
    """
    data = json.loads(request.data)
    created_at = data["created_at"]
    notification_user = data["data"]["notification_user"]
    notification_user_email = data["data"]["notification_user_email"]
    notification_type = data["data"]["notification_type"]
    notification_payload = data["data"]["notification_payload"]
    notification_id = notification_payload["notificationId"]
    level = notification_payload["event"]["level"]
    message = notification_payload["event"]["message"]
    details = notification_payload["event"]["details"]

    asyncio.run(redis_pool(data))

    slack_notification = {}
    slack_notification["blocks"] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*General Update*"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Created At*:\n{arrow.Arrow.fromtimestamp(created_at)}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Level*:\n{level}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Message*:\n{message}"
                }
            ]
        }
    ]

    if "result" in details and (details["result"]["updated_image_count"] or details["result"]["updated_record_count"]):
        slack_notification["blocks"].append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Group*:\n{details['result']['group']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Updated image count*:\n{details['result']['updated_image_count']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Status*:\n{details['result']['status']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Updated record count*:\n{details['result']['updated_record_count']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total time (seconds)*:\n{details['result']['total_time_seconds']}"
                }
            ]
        })

    slack_notification["blocks"].append({"type": "divider"})
    slack_notification["blocks"].append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*User*: {notification_user} ({notification_user_email})\n*ID*: {notification_id}"
                }
            ]
        }
    )

    # response = client.chat_postMessage(
    #     channel=SLACK_CHANNEL,
    #     blocks=slack_notification["blocks"]
    # )
    return("", 204)


@app.route("/tag_update", methods=["POST"])
def tag_update():
    """
    Anchore Engine Tag Update Slack notification webhook handler.
    """
    data = json.loads(request.data)
    asyncio.run(redis_pool(data))
    return("", 204)

@app.route("/policy_eval", methods=["POST"])
def policy_eval():
    """
    Anchore Engine Policy Eval Slack notification webhook handler.
    """
    data = json.loads(request.data)
    asyncio.run(redis_pool(data))
    return("", 204)

@app.route("/error_event", methods=["POST"])
def error_event():
    """
    Anchore Engine Error Event Slack notification webhook handler.
    """
    data = json.loads(request.data)
    asyncio.run(redis_pool(data))
    return("", 204)

@app.route("/analysis_update", methods=["POST"])
def analysis_update():
    """
    Anchore Engine Analysis Update Slack notification webhook handler.
    """
    data = json.loads(request.data)
    created_at = data["created_at"]
    notification_user = data["data"]["notification_user"]
    notification_user_email = data["data"]["notification_user_email"]
    notification_type = data["data"]["notification_type"]
    notification_payload = data["data"]["notification_payload"]
    notification_id = notification_payload["notificationId"]
    subscription_key = notification_payload["subscription_key"]
    current_evaluation = notification_payload["curr_eval"]
    current_analysis_status = current_evaluation["analysis_status"]
    last_evaluation = notification_payload["last_eval"]
    last_analysis_status = last_evaluation["analysis_status"]

    asyncio.run(redis_pool(data))
    image = _get_image_by_tag(subscription_key)
    vulnerabilities = _get_image_vulnerabilities(image["image_detail"][0]["imageId"])

    slack_notification = {}
    slack_notification["blocks"] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Analysis Update*"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Subscription Key*:\n{subscription_key}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Current Evaluation*:\n{current_analysis_status}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Created At*:\n{arrow.Arrow.fromtimestamp(created_at)}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Last Evaluation*:\n{last_analysis_status}"
                }
            ]
        }
    ]

    if vulnerabilities["vulnerabilities"]:
        high = [x for x in vulnerabilities["vulnerabilities"] if x["severity"] == "High"]
        medium = [x for x in vulnerabilities["vulnerabilities"] if x["severity"] == "Medium"]
        low = [x for x in vulnerabilities["vulnerabilities"] if x["severity"] == "Low"]
        negligible = [x for x in vulnerabilities["vulnerabilities"] if x["severity"] == "Negligible"]
        unknown = [x for x in vulnerabilities["vulnerabilities"] if x["severity"] == "Unknown"]

        slack_notification["blocks"].append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Vulnerabilities*:\nHigh: {len(high)}\nMedium: {len(medium)}\nLow: {len(low)}\nNegligible: {len(negligible)}\nUnknown: {len(unknown)}"
                }
            ]
        })

    slack_notification["blocks"].append({"type": "divider"})
    slack_notification["blocks"].append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*User:* {notification_user} ({notification_user_email})\n*ID:* {notification_id}"
                }
            ]
        }
    )

    response = client.chat_postMessage(
        channel=SLACK_CHANNEL,
        blocks=slack_notification["blocks"]
    )
    return("", 204)

def _add_repository(repository):
    payload = {}
    payload["repository"] = repository
    response = requests.post(
        f"{ANCHORE_CLI_URL}/repositories",
        params=payload,
        auth=HTTPBasicAuth(ANCHORE_CLI_USER, ANCHORE_CLI_PASS)
    )
    if response.status_code != 200:
        raise ValueError(
            f"POST /repositories {response.status_code} {response.text}"
        )
    return response.json()

def _get_image_vulnerabilities(image_id):
    response = requests.get(
        f"{ANCHORE_CLI_URL}/images/by_id/{image_id}/vuln/all",
        auth=HTTPBasicAuth(ANCHORE_CLI_USER, ANCHORE_CLI_PASS)
    )
    if response.status_code != 200:
        raise ValueError(
            f"GET /images/by_id/{image_id}/vuln/all {response.status_code} {response.text}"
        )
    return response.json()

def _get_image_by_tag(tag):
    response = requests.get(
        f"{ANCHORE_CLI_URL}/images/",
        params = {"fulltag": tag},
        auth=HTTPBasicAuth(ANCHORE_CLI_USER, ANCHORE_CLI_PASS)
    )
    if response.status_code != 200:
        raise ValueError(
            f"GET /images/?fulltag={tag} {response.status_code} {response.text}"
        )
    return response.json()[0]

async def redis_pool(data):
    redis = await aioredis.create_redis_pool(
        REDIS_URL, password=REDIS_PASS
    )
    ae = {}
    ae["data"] = pickle.dumps(data)
    result = await redis.xadd(REDIS_STREAM, fields=ae)
    redis.close()
    await redis.wait_closed()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
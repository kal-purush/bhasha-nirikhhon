#!/usr/bin/env python3
"""ussd module service
"""
from flask import Flask, request
from sendSMS import SMS


app = Flask(__name__)


@app.route('/test')
def hello():
    """checks if service is running"""
    return '', 200


@app.route('/sms', methods=['POST'])
def main():
    """Main entry point for sms service"""
    recipients = request.body.get('recipients')
    message = request.body.get('message')
    return SMS().send(recipients, message)


if __name__ == '__main__':
    app.run()
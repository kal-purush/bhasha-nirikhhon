import json
import os
import stripe

from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')
stripe.api_version = os.getenv('STRIPE_API_VERSION')

app = Flask(__name__, template_folder='client')


@app.route('/')
def order():
    api_key = os.getenv('STRIPE_PUBLIC_KEY')
    return render_template('order.html', api_key=api_key)

@app.route('/order_success')
def success():
    return render_template('order_success.html')

@app.route('/create-checkout-session', methods=['POST'])
def create_checkout_session():
    data = json.loads(request.data)
    amount = str(data['amount']).replace('.', '')
    domain_url = os.getenv('DOMAIN')

    try:
        checkout_session = stripe.checkout.Session.create(
            success_url=domain_url + "/order_success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url + "/",
            payment_method_types=["card"],
            line_items=[
                {
                    "name": data['name'],
                    "images": [domain_url + data['img']],
                    "quantity": data['quantity'],
                    "currency": data['currency'],
                    "amount": amount,
                }
            ]
        )
        return jsonify({'sessionId': checkout_session['id']})
    except Exception as e:
        return jsonify(e), 40


@app.route('/payment_intent', methods=['GET'])
def payment_intent():
    f = open("tmp/payment_intent", "r")

    content = f.read()
    return jsonify(json.loads(content))


@app.route('/webhook', methods=['POST'])
def webhook_received():
    webhook_secret = os.getenv('STRIPE_WEBHOOK_SECRET')
    request_data = json.loads(request.data)

    if webhook_secret:
        signature = request.headers.get('stripe-signature')
        try:
            event = stripe.Webhook.construct_event(
                payload=request.data,
                sig_header=signature,
                secret=webhook_secret
            )
            data = event['data']
        except Exception as e:
            return e
        event_type = event['type']
    else:
        data = request_data['data']
        event_type = request_data['type']
    data_object = data['object']

    file = open('tmp/payment_intent', 'w+')

    if event_type == 'payment_intent.failed' or data_object['status'] != 'succeeded':
        res = {
            'status': 'failed',
            'amount_received': None,
            'id': None
        }

        file.write(json.dumps(res))
        file.close()
        return jsonify(res), 200

    res = {
        'status': 'succeeded',
        'amount_received': data_object['amount_received'],
        'id': request_data['id']
    }

    file.write(json.dumps(res))
    file.close()

    return jsonify({
        'status': 'succeeded',
        'amount_received': data_object['amount_received'],
    }), 200


if __name__ == '__main__':
    app.run()
Null = None
json = {
  "version": "1.0",
  "resource": "/webinar_payment_webhook",
  "path": "/webinar_payment_webhook",
  "httpMethod": "POST",
  "headers": {
    "Content-Length": "878",
    "Content-Type": "application/json",
    "Host": "hx5us1wwe7.execute-api.ap-south-1.amazonaws.com",
    "Request-Id": "Mw7xOSi7BEiAt2",
    "User-Agent": "Razorpay-Webhook/v1",
    "X-Amzn-Trace-Id": "Root=1-65455307-6a8b284006a1bf3a502b6d13",
    "X-Forwarded-For": "52.66.75.174",
    "X-Forwarded-Port": "443",
    "X-Forwarded-Proto": "https",
    "X-Razorpay-Event-Id": "Mw7xOSi7BEiAt2",
    "X-Razorpay-Signature": "900fe49092b9eddd7cffd5d9ac9348c72d0096c9cab0cdd6f93cf6955d7a5f3e",
    "accept-encoding": "gzip"
  },
  "multiValueHeaders": {
    "Content-Length": ["878"],
    "Content-Type": ["application/json"],
    "Host": ["hx5us1wwe7.execute-api.ap-south-1.amazonaws.com"],
    "Request-Id": ["Mw7xOSi7BEiAt2"],
    "User-Agent": ["Razorpay-Webhook/v1"],
    "X-Amzn-Trace-Id": ["Root=1-65455307-6a8b284006a1bf3a502b6d13"],
    "X-Forwarded-For": ["52.66.75.174"],
    "X-Forwarded-Port": ["443"],
    "X-Forwarded-Proto": ["https"],
    "X-Razorpay-Event-Id": ["Mw7xOSi7BEiAt2"],
    "X-Razorpay-Signature": [
      "900fe49092b9eddd7cffd5d9ac9348c72d0096c9cab0cdd6f93cf6955d7a5f3e"
    ],
    "accept-encoding": ["gzip"]
  },
  "queryStringParameters": Null,
  "multiValueQueryStringParameters": Null,
  "requestContext": {
    "accountId": "047834681228",
    "apiId": "hx5us1wwe7",
    "domainName": "hx5us1wwe7.execute-api.ap-south-1.amazonaws.com",
    "domainPrefix": "hx5us1wwe7",
    "extendedRequestId": "N1npKgGHhcwEMZw=",
    "httpMethod": "POST",
    "identity": {
      "accessKey": Null,
      "accountId": Null,
      "caller": Null,
      "cognitoAmr": Null,
      "cognitoAuthenticationProvider": Null,
      "cognitoAuthenticationType": Null,
      "cognitoIdentityId": Null,
      "cognitoIdentityPoolId": Null,
      "principalOrgId": Null,
      "sourceIp": "52.66.75.174",
      "user": Null,
      "userAgent": "Razorpay-Webhook/v1",
      "userArn": Null
    },
    "path": "/webinar_payment_webhook",
    "protocol": "HTTP/1.1",
    "requestId": "N1npKgGHhcwEMZw=",
    "requestTime": "03/Nov/2023:20:07:35 +0000",
    "requestTimeEpoch": 1699042055122,
    "resourceId": "ANY /webinar_payment_webhook",
    "resourcePath": "/webinar_payment_webhook",
    "stage": "$default"
  },
  "pathParameters": Null,
  "stageVariables": Null,
  "body": {
    "entity": "event",
    "account_id": "acc_IoktOr1SiyyaML",
    "event": "payment.captured",
    "contains": ["payment"],
    "payload": {
      "payment": {
        "entity": {
          "id": "pay_Mw7xNiehFGsaoc",
          "entity": "payment",
          "amount": 100000,
          "currency": "INR",
          "status": "captured",
          "order_id": "order_Mw7xEQ1CmSfD0H",
          "invoice_id": Null,
          "international": False,
          "method": "upi",
          "amount_refunded": 0,
          "refund_status": Null,
          "captured": True,
          "description": Null,
          "card_id": Null,
          "bank": Null,
          "wallet": Null,
          "vpa": "4134134@ybl",
          "email": "fdf@gmail.com",
          "contact": "+60141413413",
          "notes": { "email": "fdf@gmail.com", "phone": "141413413" },
          "fee": 2360,
          "tax": 360,
          "error_code": Null,
          "error_description": Null,
          "error_source": Null,
          "error_step": Null,
          "error_reason": Null,
          "acquirer_data": {
            "rrn": "603589574650",
            "upi_transaction_id": "5354D9ED2A700DB20A8C998F96DE72A0"
          },
          "created_at": 1699042054,
          "upi": { "vpa": "4134134@ybl" },
          "base_amount": 100000
        }
      }
    },
    "created_at": 1699042054
  },
  "isBase64Encoded": False
}

print(json['body']['payload']['payment']['entity']['amount'])
print(json['body']['payload']['payment']['entity']['email'])
print(json['body']['payload']['payment']['entity']['contact'])


print(json['body']['payload']['payment']['entity']['method'])
print(json['body']['payload']['payment']['entity']['currency'])
print(json['body']['payload']['payment']['entity']['created_at'])
print(json['body']['payload']['payment']['entity']['vpa'])
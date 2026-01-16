import mercadopago

def gerar_link_pagamento():
    

    sdk = mercadopago.SDK("TEST-2879295092487193-120819-161acc178b785a39b74a59f20749475f-316571065")

    payment_data = {
        "items": [
            {'id': 1, 'title': 'Produtos', "quantity": 1, "currency_id": "BRL", "unit_price": 250}
        ],
        "back_urls": {
            "success": "http://127.0.0.1:8000/mercadop/compracerta/",
            "failure":"http://127.0.0.1:8000/mercadop/compraerrada/",
            "pending": "http://127.0.0.1:8000/mercadop/compraerrada/",
        },
        
        "auto_return": "all",
    }
    result = sdk.preference().create(payment_data)
    payment = result["response"]
    link_G_pagamento = payment["init_point"]
    return link_pagamento
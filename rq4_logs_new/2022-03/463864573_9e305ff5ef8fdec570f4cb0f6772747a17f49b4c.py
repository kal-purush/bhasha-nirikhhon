import uuid
from datetime import datetime
from time import sleep
from typing import Any, Dict, Tuple

import boto3
import test_utils as utils


def gen_test_event() -> Tuple[str, Dict[Any, Any]]:
    trx_id = uuid.uuid1().hex
    detail = (
        """
        {
            "customer_id": "CUS_01FWWSK432VY3X1T8A4VNYRTGR",
            "account_id": "ACC_01FWWSNA9DA3N3EQ2JHPQ4WTNR",
            "transaction_id": "%s",
            "transfer_amount": 1234.56,
            "previous_balance": 10000.00,
            "previous_avail_balance": 10000.00,
            "new_balance": 11234.56,
            "new_avail_balance" :11234.56,
            "currency": "SGD",
            "memo": "to some random account",
            "transaction_date": "2022-02-27",
            "status": "completed"
        }
    """
        % trx_id
    )

    event = {
        "Time": datetime.now(),
        "Source": "service.fund_transfer",
        "DetailType": "transfer",
        "EventBusName": "default",
        "Detail": detail,
    }

    return trx_id, event


def invoke_get_transactions_api(api_url, api_auth):
    query = """
    query test3 {
    getTransactionsForAccount(
        accountId: "%s"
    ){
        id
        sid
        amount
     }
    }
    """ % (
        utils.TEST_ACCOUNT_ID_1,
    )

    response = utils.run_query(
        api_url,
        api_auth,
        query,
    )

    return response["data"]["getTransactionsForAccount"]


def send_fund_transfer_event() -> str:
    trx_id, rand_event = gen_test_event()

    client = boto3.client("events")
    response = client.put_events(Entries=[rand_event])

    assert response["FailedEntryCount"] == 0

    return trx_id


def test_fund_transfer_event(api_url, api_auth):
    """
    send event to event bus, then check if the event has been processed
    and included in transaction query
    """
    trx_id = send_fund_transfer_event()
    # try 3 times in case the processing is slower than we expect
    for i in range(6):
        sleep(5 * i)
        transactions = invoke_get_transactions_api(api_url, api_auth)
        transaction_ids = [t["sid"] for t in transactions]
        if f"TRX_{trx_id}" in transaction_ids:
            return
        else:
            print("transactions returned:", transaction_ids)

    assert False, f"event {trx_id} not processed in time"
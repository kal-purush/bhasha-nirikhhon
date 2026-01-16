import time
from pathlib import Path
from typing import List

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import settings
import log_settings
from spapi.listings_items_api import ListingsItemsAPI
from spapi.fba_inventory_api import FBAInventoryAPI
from spapi.fba_inventory_api import FBAInventoryAPIParser


logger = log_settings.get_logger(__name__)
SCOPE = ('https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive')


class RegisterService(object):

    def __init__(self, credential_file: str) -> None:
        self.client = self._create_sheet_client(credential_file)
        self.spapi = ListingsItemsAPI()
        self.inventory = FBAInventoryAPI()

    # TODO レコードのバリデーション追加する。
    async def start_register(self, title: str, name: str, interval_sec: int=2):
        logger.info({"action": "start_register", "status": "run"})
        add = self.client.open(title).worksheet(name)
        records = add.get_all_records()

        for record in records:
            res = await self.spapi.create_new_sku(record.get("SKU"), record.get("PRICE"),
                                            record.get("ASIN"), settings.CONDITION_NOTE)
            time.sleep(interval_sec)
            if res.get("status") == "ACCEPTED":
                logger.info({"action": "start_register", "message": "register request is accepted",
                                "sku": record.get("SKU")})
                continue
            logger.error({"action": "start_register", "message": "register request is failed",
                          "value": record, "response": res})

        logger.info({"action": "start_register", "status": "done"})
            
    async def check_registerd(self, title: str, get_sheet: str, keep_sheet: str):
        logger.info({"action": "check_registerd", "status": "run"})

        add = self.client.open(title).worksheet(get_sheet)
        records = add.get_all_records()

        fnsku = {}
        for i in range(0, len(records), 50):
            skus = [record.get("SKU") for record in records[i:i+50]]
            res = await self.inventory.fba_inventory_api_v1(skus)
            res = FBAInventoryAPIParser.parse_fba_inventory_api_v1(res)
            time.sleep(10)
            if res is None:
                continue

            for r in res:
                fnsku[r["sku"]] = r["fnsku"]

        for record in records:
            record["FNSKU"] = fnsku.get(record.get("SKU"))

        rows = [list(record.values()) for record in records if record.get("FNSKU") is not None]
        db = self.client.open(title).worksheet(keep_sheet)
        db.append_rows(rows)

        for record in records:
            cell = add.find(record.get("SKU"), in_column=5)
            time.sleep(1)
            if cell:
                add.delete_row(cell.row)

        logger.info({"action": "check_registerd", "status": "done"})

    def _create_sheet_client(self, credential: str) -> gspread.Client:
        path = Path.cwd().joinpath(credential)
        keyfile = ServiceAccountCredentials.from_json_keyfile_name(path, SCOPE)
        return gspread.authorize(keyfile)
    
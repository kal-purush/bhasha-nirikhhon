from typing import List
from functools import partial
from urllib.parse import urljoin

import settings
import log_settings
from spapi.spapi import SPAPI


logger = log_settings.get_logger(__name__)


class FBAInventoryAPI(SPAPI):
    def __init__(self) -> None:
        super().__init__()

    async def fba_inventory_api_v1(self, skus) -> dict:
        return await self._request(partial(self._fba_inventory_api_v1, skus))

    def _fba_inventory_api_v1(self, skus: List[str]) -> tuple:
       logger.info({"action": "_fba_inventory_api_v1", "status": "run"})

       method = "GET"
       path = "/fba/inventory/v1/summaries"
       url = urljoin(settings.ENDPOINT, path)
       query = {
           "details": "true",
           "granularityType": "Marketplace",
           "granularityId": self.marketplace_id,
           "sellerSkus": ','.join(skus),
           "marketplaceIds": self.marketplace_id,
       }

       logger.info({"action": "_fba_inventory_api_v1", "status": "run"})
       return (method, url, query, None)


class FBAInventoryAPIParser(object):

    @staticmethod
    def parse_fba_inventory_api_v1(res: dict) -> List[dict]|None:
        logger.info({"action": "parse_fba_inventory_api_v1", "status": "run"})

        items = res.get("payload", {}).get("inventorySummaries")
        if items is None:
            logger.error({"message": "Not Founc inventory summaries"})
            return
        
        products = []
        for item in items:
            sku = item.get("sellerSku")
            fnsku = item.get("fnSku")
            products.append({"sku": sku, "fnsku": fnsku})

        logger.info({"action": "parse_fba_inventory_api_v1", "status": "done"})
        return products
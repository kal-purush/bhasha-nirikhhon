#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File  : curd_rules.py
# Author: DaShenHan&道长-----先苦后甜，任凭晚风拂柳颜------
# Author's Blog: https://blog.csdn.net/qq_32394351
# Date  : 2024/1/14

from sqlalchemy.orm import Session
from common.curd_base import CRUDBase
from ..models.vod_rules import VodRules
from typing import Optional


class CURDVodRules(CRUDBase):

    def create(self, db: Session, *, obj_in, creator_id: int = 0):
        return super().create(db, obj_in=obj_in, creator_id=creator_id)

    def search(self, db: Session, *,
               status: int = None,
               name: str = None,
               group: str = None,
               order_bys: Optional[list] = None,
               page: int = 1, page_size: int = 20) -> dict:
        filters = []
        if status is not None:
            filters.append(self.model.status == status)
        if name is not None:
            filters.append(self.model.name.like(f"%{name}%"))
        if group is not None:
            filters.append(self.model.job_group == group)

        records, total, _, _ = self.get_multi(db, page=page, page_size=page_size, filters=filters, order_bys=order_bys)

        return {'results': records, 'total': total}


curd_vod_rules = CURDVodRules(VodRules)
# -*- coding: utf-8 -*-
from odoo import fields, models


class Delivery(models.Model):
    _inherit = 'delivery.carrier'

    customs_require = fields.Boolean('Include cost of customs in export.', help="", default=False)
    customs_cost = fields.Float(string='Customs cost to include', help="Cost of customs agent")
    customs_amount = fields.Float(
        string='when the sale is greater than',
        help="Amount of order to include customs cost, expressed in the company currency.")

    def rate_shipment(self, order):
        res = super(Delivery, self).rate_shipment(order)
        is_foreign = order.partner_shipping_id.country_id != order.company_id.partner_id.country_id
        if not res['price'] or not self.customs_require or not is_foreign:
            return res
        amount = order.company_id.currency_id.compute(self.customs_amount, order.currency_id, round=False)
        if order._compute_amount_total_without_delivery() >= amount:
            cost = order.company_id.currency_id.compute(self.customs_cost, order.currency_id, round=False)
            res['price'] = res['price'] + cost
        return res
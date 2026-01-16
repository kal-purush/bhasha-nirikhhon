# -*- coding: utf-8 -*-

from odoo import models, fields, api


class CustomerContract(models.Model):
    _name = "customer.contract"

    service_id = fields.Many2one(comodel_name="product.product", string="Service")
    description = fields.Char(string="Description")
    product_uom_qty = fields.Float(string="Quantity")
    price = fields.Float(string="Price")
    discount = fields.Float(string="Discount")
    total_price = fields.Float(string="Total Price", compute="_compute_total_price")
    res_partner_id = fields.Many2one(comodel_name="res.partner", string="Res Partner")

    @api.onchange('service_id')
    def onchange__select_product(self):
        self.description = self.service_id.name
        self.price = self.service_id.lst_price

    @api.one
    @api.depends('discount')
    def _compute_total_price(self):
        discount_amount = (self.discount/100)*self.price

        self.total_price = self.price- discount_amount

# -*- coding: utf-8 -*-

from odoo import fields, api, _, models

class StockMove(models.Model):
    _inherit = 'stock.move'
    
    @api.depends('picking_id.sale_id.order_line')
    def _get_products(self): 
        products = []
        for record in self: 
            if record.picking_id.sale_id: 
                for line in record.picking_id.sale_id.order_line: 
                    products.append(line.product_id.id)
            record.select_product_ids = products
    
    select_product_ids = fields.Many2many('product.product', compute="_get_products")
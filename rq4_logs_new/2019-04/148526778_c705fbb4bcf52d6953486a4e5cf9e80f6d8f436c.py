# Copyright 2019, Jarsa Sistemas, S.A. de C.V.
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import api, fields, models

class ResPartner(models.Model):
    _inherit = 'res.partner'

    seller_ids = fields.Many2many(
        'res.users', string='Seller')

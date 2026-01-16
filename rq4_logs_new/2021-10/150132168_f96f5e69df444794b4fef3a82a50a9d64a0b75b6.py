# -*- encoding: utf-8 -*-
# Copyright 2021 Accioma (https://accioma.com).
# @author marcelomora <java.diablo@gmail.com>
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).

from odoo import _, api, fields, models

class AccountJournal(models.Model):
    _inherit = 'account.journal'

    mobilvendor_payment_type = fields.Selection([
        ('1', 'Efectivo'),
        ('2', 'Cheque'),
        ('5', 'Transferencia'),
        ('6', 'Deposito'),
        ('7', 'Tarj. Credito'),
        ('8', 'Voucher'),
        ], 'Tipo Pago Mobilvendor'
        )
    
    


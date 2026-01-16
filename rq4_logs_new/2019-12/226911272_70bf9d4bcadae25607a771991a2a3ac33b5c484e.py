# -*- coding: utf-8 -*-
from odoo import api, fields, models, SUPERUSER_ID, _
from odoo.exceptions import ValidationError
import re

class einvoice_notification_group(models.Model):
	_name = 'einvoice.notification.group'

	name = fields.Char('name')
	email = fields.Char('Email')
	company_id = fields.Many2one("res.company", "Company")

	@api.model
	def create(self, vals):
		rec = super(einvoice_notification_group, self).create(vals)
		# Check email address is valid or not
		if rec.email:
			if re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", rec.email) == None:
				raise ValidationError("Please enter a valid email address")
		return rec

	@api.multi
	def write(self, values):
		result = super(einvoice_notification_group, self).write(values)
		# Check email address is valid or not
		if values.get('email'):
			if re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$", values.get('email')) == None:
				raise ValidationError("Please enter a valid email address")
		return result
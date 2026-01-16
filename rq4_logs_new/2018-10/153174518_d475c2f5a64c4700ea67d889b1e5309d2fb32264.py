#!/usr/bin/env python
# -*- coding: utf-8 -*-
from helpers.validator import Validator
from models.pincode import Pincode

class GetCustomerLuckNumbersCommand(object):
    @staticmethod
    def call(cpf, pincode):
        if not Validator.cpf_validator(cpf): return { 'error': 'CPF inválido' }
        if not Validator.pincode_validator(pincode): return { 'error': 'Pincode inválido' }

        pincode = Pincode(pincode)
        pincode.save()

        return pincode.pincode
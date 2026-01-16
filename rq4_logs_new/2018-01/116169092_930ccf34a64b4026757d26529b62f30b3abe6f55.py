#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 17:14:10 2018

@author: lorenzozabban
"""

class BalanceCheck(object):
    

    def get_from(self, updated_blockchain, sender):
        
        _from = []
        for i in range(1, len(updated_blockchain)):
            if updated_blockchain[i].data['from'] == sender:
                _from.append(updated_blockchain[i])
            else:
                pass
        return _from
  
    
    def get_amount(self, _list):
        
        _amount = []
        for i in range(0, len(_list)):
            _amount.append(_list[i].data['amount'])
        return _amount
    
    
    def get_to(self, updated_blockchain, sender):

        _from = []
        for i in range(1, len(updated_blockchain)):
            if updated_blockchain[i].data['to'] == sender:
                _from.append(updated_blockchain[i])
            else:
                pass
        return _from
        
 
    def check(self, last_blockchain, sender, payment):
        
        amount_sent = self.get_amount(self.get_from(last_blockchain, sender))
        amount_received = self.get_amount(self.get_to(last_blockchain, sender))
        
        balance = sum(amount_received) - sum(amount_sent)

        if balance >= payment:
            return 'transaction accepted'
        else:
            return 'insufficient funds'
        
        

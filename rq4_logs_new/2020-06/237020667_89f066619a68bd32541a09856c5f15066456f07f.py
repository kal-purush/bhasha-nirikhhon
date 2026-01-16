import unittest
from sclib.agent import Agent

class Test_Agent(unittest.TestCase):
    def setUp(self):
        a1 = Agent( agent_id = 1, role = 'r', working_capital = 120.00)
        a3 = Agent( agent_id = 3, role = 'r')
        a4 = Agent( agent_id = 4, role = 'm', working_capital = 130.00)
        a6 = Agent( agent_id = 6, role = 'm')
        a7 = Agent( agent_id = 7, role = 's', working_capital = 140.00)
        a9 = Agent( agent_id = 9, role = 's')
        
        self.ret1 = a1
        self.ret3 = a3
        self.man1 = a4
        self.man3 = a6
        self.sup1 = a7
        self.sup3 = a9
  
    def test_q(self):
        self.assertEqual(self.ret1.q, 0.90)
        

    def test_retailer_is(self):
        dct = self.ret1.__dict__
        self.assertIn('consumer_demand', dct)
        self.assertIn('supplier_set', dct)
    
    def test_mutual_attrs(self):
        self.assertEqual(self.ret1.agent_id , 1 )
        self.assertEqual(self.ret1.role , 'r')
        self.assertEqual(self.ret1.working_capital , 120)        
        self.assertEqual(self.ret1.selling_price , 0)
        self.assertEqual(self.ret1.mu_selling_price , 5.00)
        self.assertEqual(self.ret1.sigma_selling_price , 0.06)
        
    def test_default_values(self):
        self.assertEqual(self.ret3.working_capital , 100)
        self.assertEqual(self.ret3.selling_price , 0)
        self.assertEqual(self.ret3.mu_selling_price , 5.00)
        self.assertEqual(self.ret3.sigma_selling_price , 0.06)
        self.assertEqual(self.ret3.consumer_demand_mean , 60.00)
        self.assertFalse(self.ret3.consumer_demand)
   
    def test_retailer_attrs(self):
        self.assertEqual(self.ret1.consumer_demand , self.ret3.consumer_demand)
        self.assertFalse(self.ret1.supplier_set)
        self.assertEqual(self.ret1.supplier_set, self.ret1.received_productions)
            
    def test_manufacturers_attrs(self):
        self.assertFalse(self.man1.supplier_set)
        self.assertFalse(self.man1.consumer_set)
        self.assertFalse(self.man1.received_orders)
        self.assertFalse(self.man1.order_quant_tracker)
        self.assertFalse(self.man1.step_production)
        self.assertFalse(self.man1.delivery_amount)
        self.assertEqual(self.man1.prod_cap, 0)
        self.assertEqual(self.man3.prod_cap, 0)
        
    def test_suppliers_attrs(self):
        self.assertFalse(self.sup1.consumer_set)
        self.assertFalse(self.sup1.received_orders)
        self.assertFalse(self.sup1.step_production)
        self.assertFalse(self.sup1.delivery_amount)
        self.assertEqual(self.sup1.prod_cap, 0)
        self.assertEqual(self.sup3.prod_cap, 0)        
        
if __name__ == '__main__':
    unittest.main()
import { Product } from "./product";
import { PricingRule } from "./pricingRule";
import { Checkout } from "./checkout";

describe('Checkout: ', () => {
  const pricingRules = [
    new PricingRule(new Product('VOUCHER', 'SNOW Voucher', 7)),
    new PricingRule(new Product('TSHIRT', 'SNOW T-Shirt', 21)),
    new PricingRule(new Product('MUG', 'SNOW Coffee Mug', 9.5)),
  ];

  describe('According to question ', () =>{
    beforeAll(() => {
      pricingRules.find((x) => x.ProductCode === 'VOUCHER').setRuleForFreeQuanities(2, 1);
      pricingRules.find((x) => x.ProductCode === 'TSHIRT').setRuleForDiscountedPrice(19, 3);
    });

    it('should return 37.5 when the item is [VOUCHER, TSHIRT, MUG]', () => {
      const co = new Checkout(pricingRules);
      co.scan('VOUCHER').scan('TSHIRT').scan('MUG');
      expect(co.total()).toBe(37.50);
    });

    it('should return 83 when the item is [TSHIRT, TSHIRT, TSHIRT, VOUCHER, TSHIRT]', () => {
      const co = new Checkout(pricingRules);
      co.scan('TSHIRT').scan('TSHIRT').scan('TSHIRT').scan('VOUCHER').scan('TSHIRT');
      expect(co.total()).toBe(83);
    });

    it('should return 80.5 when the item is [VOUCHER, TSHIRT, VOUCHER, VOUCHER, MUG, TSHIRT, TSHIRT]', () => {
      const co = new Checkout(pricingRules);
      co.scan('VOUCHER').scan('TSHIRT').scan('VOUCHER').scan('VOUCHER').scan('MUG').scan('TSHIRT').scan('TSHIRT');
      expect(co.total()).toBe(80.5);
    });
  });
});
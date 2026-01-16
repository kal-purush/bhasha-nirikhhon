describe("Vending Application", function() {

  var Accept = null;

  beforeEach(function() {
    Accept = new Accept();
  });

  describe("When No Coin is inserted", function() {
    it("Displays Insert Coin When ni coin are inserted", function() {
      expect(Accept.DisplayCredit).toEqual("INSERT COIN");
    });
  });

  
  describe("Inserted Coin", function() {
    it("Displays $ 0.25 when a Quarter is Entered", function() {
    	Accept.AcceptCoin(Cash("Quarter"));
      expect(Accept.DisplayCredit).toEqual("$0.25");
    });

    it("Displays $ 0.10 when a Quarter is Entered", function() {
    	Accept.AcceptCoin(Cash("Dime"));
      expect(Accept.DisplayCredit).toEqual("$0.10");
    });


    it("Displays the total Credit", function() {
    	Accept.AcceptCoin(Cash("Quarter"));
    	Accept.AcceptCoin(Cash("Quarter"));
      expect(Accept.DisplayCredit).toEqual("$0.50");
      	Accept.AcceptCoin(Cash("Nickel"));
      	Accept.AcceptCoin(Cash("Dime"));
      expect(Accept.DisplayCredit).toEqual("$0.15");
    });

    it("When Penny is inserted", function() {
    	Accept.AcceptCoin(Cash("Penny"));
      expect(Accept.DisplayCredit).toEqual("INSERT COIN");
    });
  });

  describe("Select Product", function() {
       
    it("Buy Candy", function() {
    	Accept.AcceptCoin(Cash("Quarter"));
    	Accept.AcceptCoin(Cash("Quarter"));
    	Accept.AcceptCoin(Cash("Nickel"));
    	Accept.AcceptCoin(Cash("Dime"));
    	Accept.selectProduct(Product("Candy"));
      expect(Accept.DisplayCredit).toBe("INSERT COIN");
    });
	
  });


});
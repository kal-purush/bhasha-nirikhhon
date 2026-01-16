require("should");

module.exports = function (data) {
    data.should.not.equal(null);
    data.should.instanceOf(Object);
    
    
    data.should.have.property('code');
    data.code.should.instanceOf(String);
    
    data.should.have.property('symbol');
    data.symbol.should.instanceOf(String);

    data.should.have.property('rate');
    data.rate.should.instanceOf(Number);
}
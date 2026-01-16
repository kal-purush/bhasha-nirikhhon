const expect = require('code').expect;   // assertion library
const Lab = require('lab');
const lab = exports.lab = Lab.script();

lab.experiment('experiment1', function(){
    
    
    lab.before(function(done){
        console.log("before test1")
        done()        
    })

    lab.before(function(done){
        console.log("beforeEach test1");
        done() 
    })
    
    
    lab.test('test1 returns true when 1 + 1 equals 2', (done) => {

        expect(1 + 1).to.equal(2);
        done();
    });
})
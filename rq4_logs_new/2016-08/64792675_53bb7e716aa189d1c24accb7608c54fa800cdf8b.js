const expect = require('chai').expect;

describe('Vault', () => {

    const config = require('../config');
    config.OUTPUT_FILE = __dirname + '/assets/test1.txt';

    const vault = require('../vault');

    it('should save the vault file without error', () => {
        return vault.writeVault('my password')
            .then(function(data) {
                expect(data).to.equal('Vault saved');
            })
            .catch(function(error) {
                console.log(error);
                expect.fail(error)
            })
    });

})
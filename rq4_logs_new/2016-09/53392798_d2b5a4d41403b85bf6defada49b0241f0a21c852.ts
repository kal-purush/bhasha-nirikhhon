import * as chai from "chai";
import {Validator} from "../../build/Validator/Index";
import {Request} from "../../build/Http/Index";
var expect = chai.expect;

describe('validator tests:', () => {
    it('it should accept request object', function (done) {
        //Given
        var request = new Request();
        var validator = new Validator();
        //When
        validator.setRequest(request);
        //Then
        expect(validator.getRequest()).to.be.deep.equal(request);
        done();
    });

    it('it should detect required parameters missing', function (done) {
        //Given
        var request = new Request();
        var validator = new Validator();
        validator.setRequest(request);
        validator.rules({
            "dummyParam": "required"
        });
        //When
        var isOk = validator.validates();
        //Then
        expect(isOk).to.be.equal(false);
        done();
    });

    it('it should detect required parameters present', function (done) {
        //Given
        var request = new Request();
        request.setPost({
            "dummyParam": "dummyValue"
        });
        var validator = new Validator();
        validator.setRequest(request);
        validator.rules({
            "dummyParam": "required"
        });
        //When
        var isOk = validator.validates();
        //Then
        expect(isOk).to.be.equal(true);
        done();
    });

    it('it should check isIn succeed', function (done) {
        //Given
        var request = new Request();
        request.setPost({
            "dummyParam": "dummy"
        });
        var validator = new Validator();
        validator.setRequest(request);
        validator.rules({
            "dummyParam": "in:some,dummy,value"
        });
        //When
        var isOk = validator.validates();
        //Then
        expect(isOk).to.be.equal(true);
        done();
    });

    it('it should check isIn fails', function (done) {
        //Given
        var request = new Request();
        request.setPost({
            "dummyParam": "dummyValue"
        });
        var validator = new Validator();
        validator.setRequest(request);
        validator.rules({
            "dummyParam": "in:some,dummy,value"
        });
        //When
        var isOk = validator.validates();
        //Then
        expect(isOk).to.be.equal(false);
        done();
    });
});
import * as chai from "chai";
import {Request} from "../../build/Http/Index";
import * as Express from "express";
var expect = chai.expect;

describe('Http Request component:', () => {
    it('should allow reading get data', function (done) {
        //Given
        var request = new Request();
        var dummyData = {somedata:"someValue"};
        //When
        request.setGet(dummyData);
        //Then
        expect(request.get()).to.be.deep.equal(dummyData);
        expect(request.get("somedata")).to.be.deep.equal("someValue");
        expect(request.get("nulldata")).to.be.deep.equal(undefined);
        done();
    });

    it('should allow reading post data', function (done) {
        //Given
        var request = new Request();
        var dummyData = {somedata:"someValue"};
        //When
        request.setPost(dummyData);
        //Then
        expect(request.post()).to.be.deep.equal(dummyData);
        expect(request.post("somedata")).to.be.deep.equal("someValue");
        expect(request.post("nulldata")).to.be.deep.equal(undefined);
        done();
    });

    it('should read data from express', function (done) {
        //Given
        var request = new Request();
        var fakeExpress = {
            params: {somegetdata: "getdata"},
            body: {somepostdata: "postdata"}
        };
        //When
        request.setFromExpress(<Express.Request>fakeExpress);
        //Then
        expect(request.post()).to.be.deep.equal(fakeExpress.body);
        expect(request.get()).to.be.deep.equal(fakeExpress.params);
        done();
    });
});
import * as chai from "chai";
import {RouteEndpoint} from "../../build/Http/Router/Route";
var expect = chai.expect;

class DummyController {
}

describe('Http Route component:', () => {
    it('should toString correctly', function (done) {
        //Given
        var endpoint = new RouteEndpoint();
        //When
        endpoint.controller = DummyController;
        endpoint.method = "testMethod";
        //Then
        expect(endpoint.toString()).to.be.equals("DummyController@testMethod");
        done();
    });
});
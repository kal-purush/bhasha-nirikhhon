/* eslint-disable */
import { expect } from "chai";
import { stub } from "sinon";
import AuthController from "../../controllers/AuthController";

describe("AuthController", () => {
    it("getConnect", () => {
        const response = {
            status: stub().returnsThis(),
            send: stub(),
        };
        AuthController.getConnect(null, response);
        expect(response.status.calledWith(200)).to.equal(true);
        expect(response.send.calledWith({ redis: true, db: true })).to.equal(true);
    });
    it("getDisconnect", () => {
        const response = {
            status: stub().returnsThis(),
            send: stub(),
        };
        AuthController.getDisconnect(null, response);
        expect(response.status.calledWith(200)).to.equal(true);
        expect(response.send.calledWith({ redis: true, db: true })).to.equal(true);
    });
    it("getMe", () => {
        const request = {
            user: {
                _id: "5f1e7d35c7ba06511e683b21",
                email: "hello@hello.com",
            },
        };
        const response = {
            status: stub().returnsThis(),
            json: stub(),
        };
        AuthController.getMe(request, response);
        expect(response.status.calledWith(401)).to.equal(false);
    });
    it("postConnect", () => {
        const request = {
            body: {
                email: "hello@xw.com",
                password: "hello",
            },
        };
        const response = {
            status: stub().returnsThis(),
            json: stub(),
        };
        AuthController.postConnect(request, response);
        expect(response.status.calledWith(401)).to.equal(false);
    });
    
});
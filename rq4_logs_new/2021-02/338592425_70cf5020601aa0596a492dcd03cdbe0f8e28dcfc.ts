import { getMockReq, getMockRes } from '@jest-mock/express';
import ChargePointController from "../../src/api/controllers/charge-point.controller";

describe('Unit test for ChargePointController', () => {

    const controller = new ChargePointController();
    
    describe('Unit test for postChargepoint function', () => {

        const { res, mockClear } = getMockRes()

        beforeEach(() => {
            mockClear();
        });

        describe('Given ChargePoint', () => {

            it('Should return 200 OK', () => {
                controller.postChargepoint(getMockReq(), res);

                expect(res.status).toHaveBeenCalledWith(200);
                expect(res.json).toHaveBeenCalledWith({message: "OK"});
            });
        });
    });

    describe('Unit test for deleteChargepoint function', () => {

        const { res, mockClear } = getMockRes()

        beforeEach(() => {
            mockClear();
        });

        describe('Given id', () => {

            it('Should return 200 OK', () => {
                controller.deleteChargepoint(getMockReq(), res);

                expect(res.status).toHaveBeenCalledWith(200);
                expect(res.json).toHaveBeenCalledWith({message: "OK"});
            });
        });
    });

    describe('Unit test for getChargepoint function', () => {

        const { res, mockClear } = getMockRes()

        beforeEach(() => {
            mockClear();
        });

        describe('Given id', () => {

            it('Should return 200 OK', () => {
                controller.getChargepoint(getMockReq(), res);

                expect(res.status).toHaveBeenCalledWith(200);
                expect(res.json).toHaveBeenCalledWith({message: "OK"});
            });
        });
    });

    describe('Unit test for putChargepoint function', () => {

        const { res, mockClear } = getMockRes()

        beforeEach(() => {
            mockClear();
        });

        describe('Given ChargePoint', () => {

            it('Should return 200 OK', () => {
                controller.getChargepoint(getMockReq(), res);

                expect(res.status).toHaveBeenCalledWith(200);
                expect(res.json).toHaveBeenCalledWith({message: "OK"});
            });
        });
    });
    
})
import {
    expect,
} from "chai";

import {
    spy,
} from "sinon";

import {
    RouteDefs,
} from "mithril";

import * as m from "mithril/render/hyperscript";

import routeRender from "./index";

describe(routeRender.name, () => {
    it(`should be a function`, () => {
        expect(routeRender).to.be.a(`function`);
    });
    it(`should return Promise`, () => {
        expect(routeRender({ "": () => `` }, "")).to.be.instanceOf(Promise);
    });
    it(`should throw error when no route has been found`, async () => {
        const catchSpy = spy();
        await routeRender({}, "").catch(catchSpy);
        expect(catchSpy.calledOnce).to.be.equal(true, `exception was not thrown`);
        expect(catchSpy.calledWith(Error)).to.be.equal(true, `exception was not an Error`);
    });
});
import {Lifetime} from "../src/Lifetime";
import {LifetimeImpl} from "../src/Lifetime";
import createSpy = jasmine.createSpy;
import Spy = jasmine.Spy;

describe("Lifetime", function() {

    it("should notify end of life listeners", function() {
        let life: LifetimeImpl = new LifetimeImpl();

        let listener1: Spy = createSpy("listener1");
        let listener2: Spy = createSpy("listener2");

        life.onEndOfLife(listener1, this);
        life.onEndOfLife(listener2, this);

        expect(listener1).not.toHaveBeenCalled();
        expect(listener2).not.toHaveBeenCalled();

        life.endLife();

        expect(listener1).toHaveBeenCalled();
        expect(listener2).toHaveBeenCalled();
    });

    it("should not allow new listeners after life is ended", function() {
        let life: LifetimeImpl = new LifetimeImpl();

        expect(() => life.onEndOfLife(() => {}, this)).not.toThrow();
        life.endLife();
        expect(() => life.onEndOfLife(() => {}, this)).toThrow();
    });

});
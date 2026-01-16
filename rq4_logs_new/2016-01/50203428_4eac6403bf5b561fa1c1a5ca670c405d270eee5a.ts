/// <reference path="../typings/browser.d.ts" />

import "./components/jsxTest";
import * as expect from "expect";

describe("basic stuff", () => {
	it("should work", () => {
		expect(true).toBe(true).toNotBe(false);		
	});
	
	it("more should work", () => {
		expect(1).toBeGreaterThan(0).toBeLessThan(2);
	});
	
	it("should be able to check the simple stuff", () => {
		expect({}).toExist();
	});
});
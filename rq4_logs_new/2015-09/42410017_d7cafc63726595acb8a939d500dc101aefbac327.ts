///<reference path="../typings/tsd.d.ts"/>

import * as chai from 'chai';
import * as lib from '../src/index';
import * as Rx from 'rx/index';
var expect = chai.expect;


var onNext = Rx.ReactiveTest.onNext,
    onCompleted = Rx.ReactiveTest.onCompleted,
    subscribe = Rx.ReactiveTest.subscribe;


describe("tests for combine | cancel primary", () => {

	it("simplest case, p-s-x, should issue single result immediately after s arrival", () => {
							
		//[p1]--------[x1]---
		//------[s1]--------
		//===================
		//------[f1]---------
		//------[s1]--------

		
		var scheduler = new Rx.TestScheduler();


		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(600, "s1"),
			onCompleted(1000)
			);
			
		var xp = scheduler.createHotObservable(
			onNext(700, "x1"),
			onCompleted(1000)
			);
			
		var res = scheduler.startWithCreate(() =>
			lib.combinator
				.combine(ps, ss, xp)
			);

		expect(res.messages).eqls(
			[onNext(600, { primary: "p1", secondary: "s1" })]
			);

	})

	it("p-x-s, should not issue any result", () => {
							
		//[p1]--[x1]---------
		//------------[s1]---
		//===================
		//-------------------
		//-------------------

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(600, "s1"),
			onCompleted(1000)
			);
			
		var xp = scheduler.createHotObservable(
			onNext(400, "x1"),
			onCompleted(1000)
			);
			
		var res = scheduler.startWithCreate(() =>
			lib.combinator
				.combine(ps, ss, xp)
			);

		expect(res.messages).eqls(
			[]
			);

	})

	it("p-s-x-s, should issue 2 results", () => {
							
		//[p1]--------[x1]--[p2]---
		//------[s1]---------------
		//=========================
		//------[p1]--------[p2]------
		//------[s1]--------[s1]----

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onNext(700, "p2"),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(400, "s1"),
			onCompleted(1000)
			);
			
		var xp = scheduler.createHotObservable(
			onNext(500, "x1"),
			onCompleted(1000)
			);
			
		var res = scheduler.startWithCreate(() =>
			lib.combinator
				.combine(ps, ss, xp)
			);
						

		expect(res.messages).eqls(
				[
					onNext(400, { primary: "p1", secondary: "s1" }),
					onNext(700, { primary: "p2", secondary: "s1" })
				]
			);

	})

}) 

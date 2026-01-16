///<reference path="../typings/tsd.d.ts"/>

import * as chai from 'chai';
import * as combinator from '../src/combinator';
import * as Rx from 'rx/index';
var expect = chai.expect;


var onNext = Rx.ReactiveTest.onNext,
    onCompleted = Rx.ReactiveTest.onCompleted,
    subscribe = Rx.ReactiveTest.subscribe;


describe("combintor test", () => {

	it("p-s => p+s after s arrival", () => {
							
		//[p1]--------
		//------[s1]--
		//============
		//------[p1]--
		//------[s1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(600, "s1"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss)
		);

		expect(res.messages).eqls(
			[
				onNext(600, { p: "p1", r: "s1" }),
				onCompleted(700)
			]
			);
	})
	
	it("p1-p2-s => p1+s1, p2+s1 after s arrival", () => {
							
		//[p1]-[p2]------------
		//-----------[s1]------
		//======================
		//-----------[p1]-[p2]--
		//-----------[s1]-[s1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onNext(400, "p2"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(600, "s1"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss)
		);

		expect(res.messages).eqls(
			[
				onNext(600, { p: "p1", r: "s1" }),
				onNext(600, { p: "p2", r: "s1" }),
				onCompleted(700)
			]
			);
	})
	
	
	it("s-p => p+s after p arrival", () => {
							
		//------[p1]--
		//-[s1]-------
		//============
		//------[p1]--
		//------[s1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(500, "p1"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(300, "s1"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss)
		);

		expect(res.messages).eqls(
			[
				onNext(500, { p: "p1", r: "s1" }),
				onCompleted(700)
			]
			);
	})
	
	it("s1-s2-p1 => p1+s2 after p arrival", () => {
							
		//------------[p1]---
		//-[s1]-[s2]---------
		//==================
		//------------[p1]--
		//------------[s2]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(500, "p1"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(300, "s1"),
			onNext(400, "s2"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss)
		);

		expect(res.messages).eqls(
			[
				onNext(500, { p: "p1", r: "s2" }),
				onCompleted(700)
			]
			);
	})
	

	it("p1-s-p2 => p1+s-p2+s", () => {
							
		//--[p1]------[p2]--
		//-------[s1]-------
		//==================
		//-------[p1]-[p2]--
		//-------[s1]-[s1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onNext(500, "p2"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(400, "s1"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss, null, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(401, { p: "p1", r: "s1" }),
				onNext(501, { p: "p2", r: "s1" }),
				onCompleted(700)
			]
			);
	})
	
	it("p1-s1-p2-s2 => p1+s1-p2+s1", () => {
							
		//--[p1]------[p2]------
		//-------[s1]-------[s2]
		//======================
		//-------[p1]-[p2]------
		//-------[s1]-[s1]-------

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onNext(500, "p2"),
			onCompleted(700)
			);


		var ss = scheduler.createHotObservable(
			onNext(400, "s1"),
			onNext(600, "s2"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, ss, null, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(401, { p: "p1", r: "s1" }),
				onNext(501, { p: "p2", r: "s1" }),
				onCompleted(700)
			]
			);
	})

	
}) 

///<reference path="../typings/tsd.d.ts"/>

import * as chai from 'chai';
import * as combinator from '../src/combinator';
import * as Rx from 'rx/index';
var expect = chai.expect;


var onNext = Rx.ReactiveTest.onNext,
    onCompleted = Rx.ReactiveTest.onCompleted,
    subscribe = Rx.ReactiveTest.subscribe;


describe("combintor test", () => {

	it("simplest case, p-x, should issue result immediately after x arrival", () => {
							
		//[p1]--------
		//------[x1]--
		//============
		//------[p1]--
		//------[x1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, "p1"),
			onCompleted(700)
			);


		var xs = scheduler.createHotObservable(
			onNext(600, "x1"),
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, (p) => xs)
		);

		expect(res.messages).eqls(
			[
				onNext(600, { p: "p1", r: "x1" }),
				onCompleted(700)
			]
			);
	})

	it("simplest case, x(replyed)-p, should issue result after p arrival + interval of x", () => {
							
		//-----------[p1]------------
		//---[x1]--------------------
		//=========================
		//------------------[p1]-----
		//------------------[x1]----

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(400, "p1"),
			onCompleted(800)
			);


		var xs = scheduler.createColdObservable(
			onNext(100, "x1"),
			onCompleted(800)
			).shareReplay(null, 1);

		/***
		 * If here we do immediately
		 * xs.subscribe(_=>_);
		 * 
		 * xs will be subscribed immediately, hence replay opertion
		 * will be invoked immediately and we will expect operation result in 400
		 * 
		 * In contrast with subscibtion made inside combine function,
		 * this case subsciption will be activated only after 400 (when p arrived)
		 * and the subscription on xs will be made, and reply will emit result 100 after,
		 * hence result time is 500 
		 *  
		 */
		
		/**
		 * if instead of shareRaply use publish().connect(), for example, then 
		 * xs will be emit value (100), in which case ps won't have time to see it  (after 400)
		 */

		var res = scheduler.startWithCreate(() =>
			combinator.combine(ps, (p) => xs)
		);
		

		expect(res.messages).eqls(
			[
				onNext(500, { p: "p1", r: "x1" }),
				onCompleted(800)
			]
			);
	})
	
	it("x-p, should issue result after p arrival", () => {
							
		//-----------[p1]------------
		//---[x1]--------------------
		//=========================
		//-----------[p1]-----
		//-----------[x1]----

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(400, "p1"),
			onCompleted(800)
			);


		var xs = scheduler.createColdObservable(
			onNext(100, "x1"),
			onCompleted(800)
			).shareReplay(1, null, scheduler);
		
		//subscribe immediately in order to start replay at one 						
		xs.subscribe(_=>_);			

		var res = scheduler.startWithCreate(() => 
			combinator.combine(ps, (p) => xs)
		);
		
		
		expect(res.messages).eqls(
			[
				onNext(401, { p: "p1", r: "x1" }),
				onCompleted(800)
			]
			);
	})


	it("x1-x2-p, should issue p1+x2 after p arrival", () => {
							
		//------------[p1]------------
		//--[x1]-[x2]-------------------
		//=========================
		//------------[p1]-----
		//------------[x2]----

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(400, "p1"),
			onCompleted(800)
			);


		var xs = scheduler.createColdObservable(
			onNext(100, "x1"),
			onNext(200, "x2"),
			onCompleted(800)
			).shareReplay(1, null, scheduler);
		
		//subscribe immediately in order to start replay at one 						
		xs.subscribe(_=>_);
		
		var res = scheduler.startWithCreate(() => 
			combinator.combine(ps, (p) => xs)
		);
		

		expect(res.messages).eqls(
			[
				onNext(401, { p: "p1", r: "x2" }),
				onCompleted(800)
			]
			);
	})

}) 

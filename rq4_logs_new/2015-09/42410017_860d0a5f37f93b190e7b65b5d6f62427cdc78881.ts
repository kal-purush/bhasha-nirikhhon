///<reference path="../typings/tsd.d.ts"/>

import * as chai from 'chai';
import * as combinator from '../src/combinator';
import * as Rx from 'rx/index';
var expect = chai.expect;


var onNext = Rx.ReactiveTest.onNext,
    onCompleted = Rx.ReactiveTest.onCompleted,
    subscribe = Rx.ReactiveTest.subscribe;


describe("combintor grouped test for 2 grups", () => {

	it("p-s => p+s after s arrival", () => {
							
		//[pa1]--------[pb1]---------
		//------[sa1]---------[sb2]--
		//===========================
		//------[pa1]---------[pb1]--
		//------[sa1]---------[sb2]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, {k : "a", v : "pa1"}),
			onNext(500, {k : "b", v : "pb1"}),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable<{k:string, v:string}>(
			onNext(400, {k : "a", v : "sa1"}),
			onNext(700, {k : "b", v : "sb1"}),
			onCompleted(1000)
			);


		var res = scheduler.startWithCreate(() => 
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);	

		expect(res.messages).eqls(
			[
				onNext(401, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa1"} }),
				onNext(701, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb1"} }),
				onCompleted(1000)
			]
			);
	})
	
	it("p1-p2-s => p1+s1, p2+s1 after s arrival", () => {
							
		//[pa1]-[pa2]----------[pb1]-[pb2]---------
		//-----------[sa1]-----------------[sb1]---
		//===============================================
		//-----------[pa1][pa2]------------[pb1][pb2]--
		//-----------[sa1][sa1]------------[sb1][sb1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, {k : "a", v : "pa1"}),
			onNext(400, {k : "a", v : "pa2"}),
			onNext(700, {k : "b", v : "pb1"}),
			onNext(750, {k : "b", v : "pb2"}),			
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(600, {k : "a", v : "sa1"}),
			onNext(800, {k : "b", v : "sb1"}),
			onCompleted(1000)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);
		
		//console.log(res.messages);

		expect(res.messages).eqls(
			[
				onNext(601, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa1"} }),
				onNext(601, { p: {k : "a", v : "pa2"}, s: {k : "a", v : "sa1"} }),
				onNext(801, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb1"} }),
				onNext(801, { p: {k : "b", v : "pb2"}, s: {k : "b", v : "sb1"} }),				
				onCompleted(1000)
			]
			);
						
	})
	
	
	it("s-p => p+s after p arrival", () => {
							
		//------[pa1]--------[pb2]--
		//-[sa1]-------[sb2]--------
		//==========================
		//------[pa1]---------[pb2]-
		//------[sa1]---------[sb2]-

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(500, {k : "a", v : "pa1"}),
			onNext(800, {k : "b", v : "pb1"}),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(300, {k : "a", v : "sa1"}),
			onNext(600, {k : "b", v : "sb1"}),
			onCompleted(1000)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(501, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa1"} }),
				onNext(801, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb1"} }),
				onCompleted(1000)
			]
			);
	})
	
	it("s1-s2-p1 => p1+s2 after p arrival", () => {
							
		//------------[pa1]-----------------[pa1]-
		//-[sa1]-[sa2]--------[sa1]-[sa2]---------
		//========================================
		//------------[pa1]-----------------[pa1]-
		//------------[sa2]-----------------[sa2]-

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(400, {k : "a", v : "pa1"}),
			onNext(700, {k : "b", v : "pb1"}),
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(200, {k : "a", v : "sa1"}),
			onNext(300, {k : "a", v : "sa2"}),
			onNext(500, {k : "b", v : "sb1"}),
			onNext(600, {k : "b", v : "sb2"}),			
			onCompleted(1000)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(401, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa2"} }),
				onNext(701, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb2"} }),
				onCompleted(1000)
			]
			);
	})
	

	it("p1-s-p2 => p1+s-p2+s", () => {
							
		//--[pa1]------[pa2]------[pb1]--------[pb2]--
		//-------[sa1]--------------------[sb1]-------
		//=============================================
		//-------[pa1]-[pa2]--------------[pb1]-[pb2]--
		//-------[sa1]-[sa1]--------------[sb1]-[sb1]--

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, {k : "a", v : "pa1"}),
			onNext(500, {k : "a", v : "pa2"}),
			onNext(700, {k : "b", v : "pb1"}),
			onNext(900, {k : "b", v : "pb2"}),			
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(400, {k : "a", v : "sa1"}),
			onNext(800, {k : "b", v : "sb1"}),
			onCompleted(1000)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(401, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa1"} }),
				onNext(501, { p: {k : "a", v : "pa2"}, s: {k : "a", v : "sa1"} }),
				onNext(801, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb1"} }),
				onNext(901, { p: {k : "b", v : "pb2"}, s: {k : "b", v : "sb1"} }),				
				onCompleted(1000)
			]
			);
	})
	
	it("p1-s1-p2-s2 => p1+s1-p2+s1", () => {
							
		//--[pa1]------[pa2]--------[pb1]------[pb2]--------
		//-------[sa1]-------[sa2]-------[sb1]-------[sb2]--
		//=================================================
		//-------[pa1]-[pa2]-------------[pa1]-[pb2]-------
		//-------[sa1]-[sa1]-------------[sa1]-[sb1]-------

		
		var scheduler = new Rx.TestScheduler();

		var ps = scheduler.createHotObservable(
			onNext(300, {k : "a", v : "pa1"}),
			onNext(500, {k : "a", v : "pa2"}),
			onNext(600, {k : "b", v : "pb1"}),
			onNext(800, {k : "b", v : "pb2"}),			
			onCompleted(1000)
			);


		var ss = scheduler.createHotObservable(
			onNext(400, {k : "a", v : "sa1"}),
			onNext(550, {k : "a", v : "sa2"}),
			onNext(700, {k : "b", v : "sb1"}),
			onNext(900, {k : "b", v : "sb2"}),			
			onCompleted(700)
			);


		var res = scheduler.startWithCreate(() =>
			combinator.combineGroup(ps, ss, (i) => i.item.k, scheduler)
		);

		expect(res.messages).eqls(
			[
				onNext(401, { p: {k : "a", v : "pa1"}, s: {k : "a", v : "sa1"} }),
				onNext(501, { p: {k : "a", v : "pa2"}, s: {k : "a", v : "sa1"} }),
				onNext(701, { p: {k : "b", v : "pb1"}, s: {k : "b", v : "sb1"} }),
				onNext(801, { p: {k : "b", v : "pb2"}, s: {k : "b", v : "sb1"} }),
				onCompleted(1000)
			]
			);
	})

	
}) 

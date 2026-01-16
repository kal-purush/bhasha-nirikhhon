/// <reference path="./typings/node/node.d.ts" />
/// <reference path="./typings/mocha/mocha.d.ts" />

import assert = require("assert");
import vehicle = require('./vehicle');
import moto = require('./moto');
import drive = require('./drive');
import dummyDriver = require('./dummyDriver');
import stubDriver = require('./stubDriver');
import spyVehicle = require('./spyVehicle');
import fakeDriver = require('./fakeDriver');
import mockDriver = require('./mockDriver');

var motogp = new moto.moto();
var carro = new vehicle.mocking.vehicle();

var dummyDriver1 = new dummyDriver.dummyDriver();
var stubDriver1 = new stubDriver.stubDriver();
var spyVehicle1 = new spyVehicle.spyVehicle();
var fakeDriver1 = new fakeDriver.fakeDriver();
var mockDriver1 = new mockDriver.mockDriver();

describe('Should Stop Engine (Dummy)',function(){
	it('Should return false',function(){
		
		/*
		Dummy
		
		Dummy does no action, returns whats closest to 0 or null
		Placeholder required to pass test
		*/
		var result = drive.drive.stopVehicle(motogp,dummyDriver1);
		
		assert.equal(false,result);
	})
})

describe('Should Start Engine (Stub)',function(){
	it('Should return true',function(){
		
		/*
		Stub
		
		Stub is a step up from a dummy
		Implements an interface but returns canned responses
		
		In this case our stub always returns A as License Type
		
		*/
		var result = drive.drive.startVehicle(motogp,stubDriver1);
		
		assert.equal(true,result);
	})
})

describe('Count Number of Starts Engine (Spy)',function(){
	it('Should return 2',function(){
		
		/*
		Spy
		
		Spy type of stub, does no external action, remembers certain function, what function was called, 
		how many times was it called, records facts of their invocation
		
		In this example our spy keeps a record of how many times the engine is started
		
		*/
		drive.drive.startVehicle(spyVehicle1,stubDriver1);
		drive.drive.startVehicle(spyVehicle1,stubDriver1);
		drive.drive.startVehicle(spyVehicle1,stubDriver1);
		
		var result = spyVehicle1.getNumberOfEngineStarts();
		
		assert.equal(3,result);
	})
})

describe('Clean Vehicle (Fake)',function(){
	it('Should return true',function(){
		
		/*
		Fake
		
		Fakes are a step up from a stub, they do not only return a canned response but have working
		implementations but usually take some shortcut
		
		In this case our fake not only returns A when ask for a license type but also implements a working
		clean vehicle method which of course takes a shortcut to accomplish

		*/
		fakeDriver1.CleanVehicle(carro);
						
		var result = carro.getIsCarClean();
		
		assert.equal(true,result);
	})
})


describe('Clean Car (Mock)',function(){
	it('Should Not throw exception',function(){
		
		/*
		Mock
		
		Mocks are pre-programmed with expectations which form a specification of the calls they are expected to receive. 
		Mocks test behavior
		
		For our mock, it expects to receive true when CleanVehicle is called, if not then mockDriver throws expection, for out 
		test to pass we need to check that no exception is thrown

		*/		
		
		assert.doesNotThrow(function(){
			mockDriver1.CleanVehicle(carro,true);
		});
							
	})
})


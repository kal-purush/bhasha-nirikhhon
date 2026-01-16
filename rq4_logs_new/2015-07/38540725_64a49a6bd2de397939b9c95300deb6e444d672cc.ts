import Rx = require('rx');
import * as utils from './utils';

export class RxBackpressure {

	zip() {
		var vals = Rx.Observable.range(1,10);
		var ticks = Rx.Observable.interval(1000);
		var source = Rx.Observable.zip(vals, ticks, (v,t) => v);
		source.subscribe(utils.CreateDumpingObserver());	
	}
	
	throttle() {
		var ticks = Rx.Observable.interval(100);
		var slower = ticks.throttleFirst(1000);
		slower.subscribe(utils.CreateDumpingObserver('throttle'));
	}
	
	//this crashes the stack
	generate() {
		var generator = Rx.Observable.generate<number,number>(1, 
						(v:number) => true,
						(v:number) => v + 1,
						(v:number) => v * v,
						Rx.Scheduler.immediate)
						
					.take(20);
		
		var sub = generator.subscribe(utils.CreateDumpingObserver("generator"));
	}
	
	sample() {
		var ticks = Rx.Observable.interval(100);
		var slower = ticks.sample(1000);
		slower.subscribe(utils.CreateDumpingObserver('sample'));
	}
	
	bufferWithCount() {
		var ticks = Rx.Observable.interval(10);
		var slower = ticks.map(v => v + 1).bufferWithCount(10).take(10);
		slower.subscribe(utils.CreateDumpingObserver('buffer'));		
	}
	
}
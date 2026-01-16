import { AUTO_CONTROL_FREQUENCY } from '../globals/uris';
import { DymoStore } from '../io/dymostore';
import { Control } from '../model/control';
import { Ramp } from '../util/ramp';

export var UNAVAILABLE = "unavailable";
export var CALIBRATING = "calibrating";

/**
 * Sensor controls that read their values from sensors.
 */
export class SensorControl extends Control {

	private $scope;
	private $ngSensor;
	private sensorName;
	private watchFunctionName;
	private updateFunction;
	private resetFunction;
	private options;
	private referenceValue;
	private referenceAverageOf;
	private averageOf;
	private previousValues;
	private previousUpdateTime;
	private ramp;
	private status = UNAVAILABLE;

	private watch;

	constructor(uri, sensorName, watchFunctionName, updateFunction, store: DymoStore, resetFunction?: Function, options?: Object) {
		super(uri, uri, uri, store);
		this.watchFunctionName = watchFunctionName;
		this.updateFunction = updateFunction;
		this.resetFunction = resetFunction;
		this.options = options;
	}

	getStatus() {
		return this.status;
	}

	getReferenceValue() {
		return this.referenceValue;
	}

	setReferenceAverageOf(count) {
		this.referenceAverageOf = count;
		this.resetReferenceValueAndAverage();
	}

	setAverageOf(count) {
		this.averageOf = count;
		this.resetReferenceValueAndAverage();
	}

	setSmooth(smooth) {
		if (smooth) {
			this.ramp = new Ramp();
		} else {
			this.ramp = undefined;
		}
	}

	resetReferenceValueAndAverage() {
		this.referenceValue = undefined;
		this.resetValue();
		this.status = UNAVAILABLE;
		this.previousValues = [];
	}

	startUpdate() {
		if (!this.options) {
			var freq = this.store.findControlParamValue(this.uri, AUTO_CONTROL_FREQUENCY);
			this.options = { frequency: freq? freq: 100 };
		}
		if (this.$ngSensor) {
			this.watch = this.$ngSensor[this.watchFunctionName](this.options);
			this.watch.then(null, this.onError, result => {
				this.updateFunction(result);
				/*if (this.$scope) {
					//scope apply here whenever something changes
					setTimeout(function() {
						this.$scope.$apply();
					}, 10);
				}*/
			});
		} else {
			console.log(this.uri + " " + UNAVAILABLE);
		}
	}

	update(newValue) {
		this.status = CALIBRATING;
		//still measuring reference value
		if (this.referenceAverageOf && this.previousValues.length < this.referenceAverageOf) {
			this.previousValues.push(newValue);
			//done collecting values. calculate average and adjust previous values
			if (this.previousValues.length == this.referenceAverageOf) {
				this.referenceValue = this.getAverage(this.previousValues);
				this.previousValues = this.previousValues.map(a => a - this.referenceValue);
			}
		//done measuring. adjust value if initialvalue taken
		} else {
			//take value relative to referenceValue (optional)
			if (this.referenceValue) {
				newValue -= this.referenceValue;
			}
			//take average of last averageOf values (optional)
			if (this.averageOf) {
				this.previousValues.push(newValue);
				//remove oldest unneeded values
				while (this.previousValues.length > this.averageOf) {
					this.previousValues.shift();
				}
				//calculate average
				if (this.previousValues.length == this.averageOf) {
					newValue = this.getAverage(this.previousValues);
				}
			}
			//update via smoothing ramp (optional)
			if (this.ramp) {
				var now = Date.now();
				var duration;
				if (!this.previousUpdateTime) {
					duration = 1000; //one second initially
				} else {
					duration = now-this.previousUpdateTime;
				}
				this.startUpdateRamp(newValue, duration);
				this.previousUpdateTime = now;
			} else {
				//call regular super method
				this.updateValue(newValue);
			}
		}
	}

	private getAverage(list) {
		var sum = list.reduce((a, b) => a + b);
		return sum / list.length;
	}

	private startUpdateRamp(targetValue, duration) {
		var frequency = this.store.findControlParamValue(this.uri, AUTO_CONTROL_FREQUENCY);
		this.ramp.startOrUpdate(targetValue, duration, frequency? frequency: 100, value => {
			//call regular super method
			this.updateValue(value);
		});
	}

	getSensorName() {
		return this.sensorName;
	}

	getSensor() {
		return this.$ngSensor;
	}

	getScope() {
		return this.$scope;
	}

	setScopeNgSensorAndStart(scope, ngSensor) {
		this.$scope = scope;
		this.$ngSensor = ngSensor;
		// Wait for device API libraries to load (Cordova needs this)
		document.addEventListener("deviceready", this.startUpdate, false);
	}

	reset() {
		if (this.watch) {
			this.$ngSensor.clearWatch(this.watch);
			this.watch = null;
			if (this.resetFunction) {
				this.resetFunction();
			}
		}
	}

	private onError() {
		console.log(this.uri + ' control error!');
	}

}
import Activity from "../Activity.js";
import ActivityTable from "../ActivityTable.js";

//example test
class Calculator{
	add(a,b){
		return a + b;
	}
	subtract(a,b){
		return a - b;
	}
}
		
//describe(description, specDefinitions)
//specDefinitions is simply a function containing tests for your class
describe("calculatorSuite", () => {
	const calculator = new Calculator();
	
	//it(description, testFunction *optional*, timeout *optional*)
	it("should add two numbers together", () => {
		//expect(thing).toBe(realThing);
		//other versions of toBe:   toBeGreaterThan, toBeLessThan, https://jasmine.github.io/api/3.3/matchers.html
		expect(calculator.add(2,2)).toBe(4);
	});
	
	it("should subtract two numbers from eachother", () => {
		expect(calculator.subtract(2,2)).toBe(0);
	});
});


//Real Tests
describe("ActivityTableSuite", () => {
	const activity1 = new Activity(true, 1, 2);
	const activity2 = new Activity(false, 2, 3);
	const activities = [];
	activities.push(activity1);
	activities.push(activity2);
	const activityTable = new ActivityTable(activity1);
	
	it("should have at least one activity", () => {
		expect(activityTable.getTable().length).toBeGreaterThan(0);
	});
	
	it("should sum its activities' stationary times into a total stationary duration", ()=>{
		expect(activityTable.getStationaryDuration()).toBe(1);
	});
	
	it("should sum its activities' active times into a total active duration", ()=>{
		expect(activityTable.getActiveDuration()).toBe(1);
	});
});
		
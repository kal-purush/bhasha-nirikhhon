///<reference path="cucumber.d.ts"/>

interface MyInputData {
  foo: string;
  bar: string;
}

function wrapper() {

  // We must use functions and not lambdas to define steps because 'this' will point to the world object.

  this.Before(function (scenario: cucumber.Scenario, callback: cucumber.StepCallback) {
    var scenarioName: string = scenario.getName();
    callback();
  });

  this.Given(/^Some input like this:$/, function (input: string, callback: cucumber.StepCallback) {
    if (input.length < 10)
      callback('Input be at least 10 characters');
    else
      callback();
  });

  this.When(/Something happens/, function (callback: cucumber.StepCallback) {
    // This step is not yet implemented.
    callback.pending();
  });

  this.When(/^A big table of data:$/, function (table: cucumber.DataTable<MyInputData>,
                                                callback: cucumber.StepCallback) {
    if (!table) {
      return callback('Argh!  The table is empty!');
    }

    var inputs: MyInputData[] = table.hashes();

    var mashup: string = inputs.map((row: MyInputData): string => { return row.foo + row.bar; }).join('\n');

    callback();
  });

}

export = wrapper;
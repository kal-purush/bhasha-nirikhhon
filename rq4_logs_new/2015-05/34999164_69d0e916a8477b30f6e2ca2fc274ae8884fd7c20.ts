/// <reference path="typeDefinitions/knockout.d.ts" />
/// <reference path="viewModel.ts" />
/// <reference path="sumGenerator.ts" />

var sumGenerator = new Sums.SumGenerator();
var additionViewModel = new LongArithmatic.ViewModel(sumGenerator);
ko.applyBindings(additionViewModel);
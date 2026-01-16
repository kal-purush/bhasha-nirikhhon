/// <reference path="../node_modules/lz-tslib-interfaces/FPredicate" />
/// <reference path="../node_modules/lz-tslib-interfaces/FAggregator" />
/// <reference path="../node_modules/lz-tslib-interfaces/FFunc" />

export function remove<T>(
	array: Array<T>,
	matchPredicate: FPredicate<T>) {
	for (var index: number = array.length - 1; index >= 0; index--) {
		if (matchPredicate(array[index])) {
			array.splice(index);
		}
	}
}

export function select<T, P>(
	array: Array<T>,
	propertySelector: FFunc<T, P>): Array<P> {

	var selected: Array<P> = [];
	
	for (var index = 0; index < array.length; index++) {
		var element = array[index];
		selected.push(propertySelector(element));
	}
	
	return selected;
}

export function aggregate<T>(
	array: Array<T>,
	aggregator: FAggregator<T>): T {

	var aggregation: T;

	array.forEach(function (item) {
		aggregation = aggregator(aggregation, item);
	});

	return aggregation;
}
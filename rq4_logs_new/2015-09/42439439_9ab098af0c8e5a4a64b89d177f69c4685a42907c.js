QUnit.test( "chartLow", function( assert ) {
	var chart = new BarChart({
		// chartLow: 0,
		data: [
			{
				label: 'UK Average',
				value: [
					{value: 50, label: 'Gas (total from bills)'},
					{value: 80, label: 'Space heating'}
				]
			},
			{
				label: 'Negative stacked',
				value: [
					{value: -150, label: 'Gas (total from bills)'},
					{value: 80, label: 'Space heating'}
				]
			},
			{
				label: 'Label 1',
				value: [
					{value: 200, label: 'Gas (total from bills)'},
					{value: 120, label: 'Space heating'},
					{value: 65, label: 'Total primary energy'},
					{value: 30, label: 'Solid fuel (total from bills)'}
				]
			},
			{label: 'Your home now (model)', value: -80},
			{label: 'Carbon Coop 2050 target', value: 125}
		]
	});

	assert.equal( chart.chartLow(), -150 );
});
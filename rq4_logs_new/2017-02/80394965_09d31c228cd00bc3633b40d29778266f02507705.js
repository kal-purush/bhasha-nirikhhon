QUnit.test("a basic test example", function(assert)
{
	var value = "hello";
	assert.equal( value, "hello", "We expect value to be hello" );
	
	var $div = $(document.createElement('div'));
	
	$div.globalHover(function(){}, function(){ });
});
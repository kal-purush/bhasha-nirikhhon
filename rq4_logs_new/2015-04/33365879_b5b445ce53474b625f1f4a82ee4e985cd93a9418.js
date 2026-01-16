//Fix Comments
//2015 - Logan Hampton

app.addToolButton({
	cName: "Fix Comments",
	cExec: "fixComments()",
	cTooltext: "Fix Comments",
	cEnable: true,
	cLabel: "Fix Comments"
});


var fixComments = app.trustedFunction(
    function() {
    	app.alert("Button works!")
    });
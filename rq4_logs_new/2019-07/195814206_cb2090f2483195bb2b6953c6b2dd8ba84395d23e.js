function showOnConsole(val)
{
	document.getElementById("res").value+=val;
}
function clearConsole()
{
	document.getElementById("res").value="";
}
function calculate()
{
	document.getElementById("res").value=eval(document.getElementById("res").value);
}
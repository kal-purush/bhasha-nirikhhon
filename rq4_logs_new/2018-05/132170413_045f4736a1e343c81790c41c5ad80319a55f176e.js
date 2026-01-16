function bloom()
{
	var rect = document.getElementById("rectangle");
	var width = rect.clientWidth;
	var height = rect.clientHeight;

	rect.style.width = width*5/4 + 'px';
	rect.style.height = height*5/4 + 'px';
}
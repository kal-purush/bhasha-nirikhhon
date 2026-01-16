

function hide()
{
	var elements = document.getElementsByClassName("questions");
	for (var i = 0, len = elements.length; i < len; i++)
	{
		elements[i].style.display = "none";
	}
}

function show()
{
	var elements = document.getElementsByClassName("questions");
	for (var i = 0, len = elements.length; i < len; i++)
	{
		elements[i].style.display = "block";
	}
}

function hideAll()
{
	var elements = document.getElementsByClassName("questions");
	for (var i = 1, len = elements.length; i < len; i++)
	{
		elements[i].style.display = "none";
	}
	hideAllExecuted = true;
}


function nextQuestion(qNum)
{



	var elements = document.getElementsByClassName("questions");
	var cQNum = qNum - 1;
	if (qNum == 0)
		cQNum = 9;
	if (qNum == 7)
	{
		var qValue = document.getElementsByName("question7.1");
		for (var i = 0, length = qValue.length; i < length; i++)
		{
			if (qValue[i].checked)
			{
				if (qValue[i].value == 200)
					qNum = 8;
				break;
			}
		}
	}

	elements[cQNum].style.display = "none";
	elements[qNum].style.display = "block";

}



function changeImage(imgNum)
{
var img = document.getElementById("prog-bar");
var imgNum;

if (imgNum == 1){
	img.src="https://puu.sh/zRygD/458a258d6d.png";
}

if (imgNum == 2){
	img.src="https://puu.sh/zRygM/21f5427565.png";
}

if (imgNum == 3){
	img.src="https://puu.sh/zRyhi/7a799ec53a.png";
}

if (imgNum == 4){
	img.src="https://puu.sh/zRyhr/3bae105673.png";
}

if (imgNum == 5){
	img.src="https://puu.sh/zRyhy/7239c47b36.png";
}

if (imgNum == 6){
	img.src="https://puu.sh/zRyi0/e1aeae2231.png";
}

if (imgNum == 7){
	img.src="https://puu.sh/zRyhE/b170853537.png";
}

if (imgNum == 8){
	img.src="https://puu.sh/zRyib/12edd712b7.png";

}

if (imgNum == 9){
	img.src="https://puu.sh/zRyih/96289ef665.png";
}

if (imgNum == 10){
	img.src="https://puu.sh/zRyio/5126e8a2ed.png";
}

}

function submitAnswers()
{
	var totalRadios = document.getElementsByClassName("option");
	var totalValue = 0;
	for (var i = 0, length = totalRadios.length; i < length; i++)
	{
		if (totalRadios[i].checked)
		{
			var midTerm = Number(totalRadios[i].value);
			totalValue += midTerm;
		}
	}
	alert("Total Score: " + totalValue);
}
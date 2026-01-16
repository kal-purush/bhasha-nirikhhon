

function count(){
	var quest1 = document.test.quest1.value;
	var quest2 = document.test.quest2.value;
	var quest3 = document.test.quest3.value;
	var correct = 0;

	if (quest1 == "forest")  {
		correct++;
}
	if (quest2 == "magnetic resonance imaging") {
		correct++;
}	
	if (quest3 == "old people") {
		correct++;
	}
	var images = ["img/joy.jpg", "img/bigEyes.jpg", "img/cry.jpg"];
	var infos = ["Perfect!", "More or less!", "Tragedy!"];	
	var score;

	if (correct == 0) {
		score = 2;
	}

	if (correct > 0 && correct < 3) {
		score = 1;
	}

	if (correct == 3) {
		score = 0;
	}

	document.getElementById("after_submit").style.visibility = "visible";		
	document.getElementById("info").innerHTML = infos[score];
	document.getElementById("number_correct").innerHTML = "Score: <br>" + correct + " correct answer(s)! - " + Math.round((correct/3)*100) + "%";
	document.getElementById("image").src = images[score];
	
	}
	

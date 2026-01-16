var questions = [
	new Question("Which of the following is not object oriented ?",["Java", "C", "C++", "PYTHON"], "b"),
	new Question("Which is the detective based cartoon ?", ["Scooby Doo", "Jerry", "Phines", "Pokemon" ], "a"),
	new Question("Quantum mechanics based greatest scientist...", ["Mitchio Kaku", "Galileo Galilei", "Albert Einstein", "Newton"], "c"  ),
	new Question("Which company does ps4 belongs to ....", ["Samsung", "Sony", "Xbox", "BlackBerry" ], "b" ),
	new Question("Paul Pogba is presently in which club ?", ["Real Madrid", "Barcelona", "Athletico Madrid", "Juventus"], "d"  )
];

var quiz = new Quiz(questions);


function populate(){

	if(quiz.isEnded()){

	}else{
		var el = document.getElementById("question");
			el.innerHTML = quiz.getQuestionIndex().text;
	}

}


populate();
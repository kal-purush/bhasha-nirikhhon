var container = document.getElementById("game-container");
var button1 = document.getElementById("button1");
var button2 = document.getElementById("button2");
var button3 = document.getElementById("button3");
var description = document.getElementById("description")
var inventoryItem = document.getElementById("inventoryItem")
var title = document.getElementById("title")
var backGroundImage = document.body.style.backgroundImage;

button1.onclick = gengarMachamp;
button2.onclick = pikachuDead;
button3.onclick = quickAttack;

function refresh() {
	location.reload();
}

function gengarMachamp() {
	button3.disabled = false;
	
	title.innerHTML = "Elite Four: Bruno"
	
	description.innerHTML = "Je deed een super effective move tegen Lorelei's Lapras en Lapras ging dood. Je ging door de volgende deur en kwam tegen bruno de fighting type master te staan vechten, Bruno heeft zijn Machamp, je hebt je Mega-Gengar klaar staan, wat doe je?"
	
	document.body.style.backgroundImage = "url('images/Gengar machamp.png')"

	button1.innerHTML = button1.innerHTML = "Curse"
	button2.innerHTML = button2.innerHTML = "Shadow Ball"
	button3.innerHTML = button3.innerHTML = "Psychic"

	button1.onclick = gengarDead;
	button2.onclick = shadowBall;
	button3.onclick = Psychic;


	button3.style.backgroundColor = "green"
}

function quickAttack() {
	button3.disabled = true;

	description.innerHTML = "Je liet Pikachu Quick Attack gebruiken, hij hitte Lapras en lapras deed een Ice Beam wat Pikachu maar net mistte, Wat wil je nu doen?"
	
	if (button3.disabled = true) {
		button3.style.backgroundColor = "red"
	}
}

function Psychic() {
	title.innerHTML = "Elite Four: Agatha"

	description.innerHTML = "Je deed Psychic, dat was super effective op Machamp, Machamp ging dood en je ging weer door de deur, daar stond Agatha de ghost type master, ze heeft haar Gengar op het veld staan, jij hebt je Mega Gengar er ook staan, wat wil je doen?"
	
	document.body.style.backgroundImage = "url('images/Gengar gengar.png')"

	button3.onclick = psychicGengar;
	button2.onclick = gengarShadowBall;
	button1.onclick = gengarCurse;

	button2.style.backgroundColor = "green"
	button3.style.backgroundColor = "green"
}

function gengarCurse() {
	button1.disabled = true;
	
	description.innerHTML = "Je probeerde weer eens Curse, je verloor half je HP om Gengar te cursen, Gengar deed Calm mind, je voelt je opgelucht"
	
	button2.onclick = gengarShadowBallKill;
	button3.onclick = psychicGengar;
	button1.onclick = explosion;
	
	if (button1.disabled = true) {
		button1.style.backgroundColor = "red"
	}
}

function gengarShadowBallKill() {
	title.innerHTML = "Elite Four: Lance"
	
	button1.disabled = false;
	
	document.body.style.backgroundImage = "url('images/Golem dragonite.png')"
	
	button1.innerHTML = button1.innerHTML = "Explosion"
	button2.innerHTML = button2.innerHTML = "Rock Slide"
	button3.innerHTML = button3.innerHTML = "Earthquake"
	
	description.innerHTML = "Je deed shadowball, dat killde Agatha's Gengar nog net niet, Agatha's Gengar deed Thunderbolt, dat was niet effective op Gengar. Toen kwam de curse en Agatha's Gengar ging dood. Je voelt je net als een geluksvogel, je gaat door de deur en Lance staat daar, Je hebt je Golem klaar staan tegen zijn Dragonite, wat ga je doen?"
	
	button1.style.backgroundColor = "green"

}
function gengarCurseHalfHp() {
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	description.innerHTML = "Je probeerde nog een keer Curse, Daardoor verloor je Half je HP, maar omdat je Gengar net op het punt stond om al dood te gaan, verloor je AL je HP en ging dood."
	
	if (button2.disabled == true || button3.disabled == true) {
		button2.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
}

function gengarShadowBall() {
	button2.disabled = true;
	
	description.innerHTML = "Je gebruikte Shadow Ball, Omdat Gengar half ghost type is, was het super effective op Agatha's Gengar, maar ging nog net niet dood, gelukkig gebruikte Agatha's gengar Calm Mind, wat geen damage doet op Pokémons, wat doe je nu?"
	
	if (button2.disabled == true) {
		button2.style.backgroundColor = "red"
	}
}


function psychicGengar() {
	button1.disabled = false;
	button2.disabled = false;
	
	title.innerHTML = "Elite Four: Lance"
	
	document.body.style.backgroundImage = "url('images/Golem dragonite.png')"
	
	description.innerHTML = "Je deed Psychic, dat was ook super effective op Gengar, Gengar ging dood en je ging weer door de deur, Lance, de dragon type master. Je hebt Golem klaarstaan tegen Lance's Dragonite. Wat ga je doen?"
	
	button1.innerHTML = button1.innerHTML = "Explosion"
	button2.innerHTML = button2.innerHTML = "Rock Slide"
	button3.innerHTML = button3.innerHTML = "Earthquake"
	
	button1.onclick = explosion;
	button2.onclick = rockSlide;
	button3.onclick = earthquake;
	
	button1.style.backgroundColor = "green"
	button2.style.backgroundColor = "green"
}
function earthquake() {
	description.innerHTML = "Je deed earthquake, omdat Dragonite een Flying type is, heeft ground type attacks geen effect op hem, Dragonite deed dragon dance, dragonite's snelheid en kracht is verdubbeld!"
	
	button3.disabled = true;
	if (button3.disabled == true) {
		button3.style.backgroundColor = "red"
	}
}

function rockSlide() {
	button3.disabled = false;
	
	title.innerHTML = "Elite Four: Champion!"
	
	description.innerHTML = "Je gebruikte Rock Slide, dat was super effective op Dragonite. Je ging door de grote deur en daar stond de champion en je ultieme rivaal: Blue. Hij heeft zijn Blastoise en jij je Charizard, Wat ben je van plan te gaan doen?"
	
	document.body.style.backgroundImage = "url('images/Blastoise.png')"
	
	button1.innerHTML = button1.innerHTML = "Fire spin"
	button2.innerHTML = button2.innerHTML = "Mega Punch"
	button3.innerHTML = button3.innerHTML = "Fire Blast"
	
	button2.onclick = megaPunchStory;
	button1.onclick = fireSpin;
	button3.onclick = fireBlast;
	
	button3.style.backgroundColor = "green"
}

function megaPunchStory() {
	button1.onclick = fireSpinStory;
	button3.onclick = fireBlastPotion
	
	button2.disabled = true;
	
	description.innerHTML = "Charizard deed mega punch, maar dat had niet zo veel effect op Blastoise, vervolgens deed Blastoise hydro pump, Charizard heeft het nog net aan overleefd. Door de vaardigheid vaan Charizard's " + "Tough Claws. " + "Is de kracht van Charizard verdubbeld met 30%. "
	
	if (button2.disabled == true) {
		button2.style.backgroundColor = "red"
	}
}

function fireBlastStory() {
	button1.innerHTML = button1.innerHTML = "Play Again"
	
	button1.onclick = refresh;
	
	button1.disabled = false;
	button3.disabled = true;
	
	document.body.style.backgroundImage = "url('images/Team.png')"
	
	description.innerHTML = "Charizard gaf alles wat hij in zich had voor deze fire blast, het raakte Blastoise en Blastoise werd verslagen. Je bent de Kampioen van Kanto geworden!"
	
	if (button1.disabled == true || button3.disabled == true) {
		button1.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
	button1.style.backgroundColor = "green"

}

function fireSpinStory() {
	button3.onclick = fireBlastStory;
	
	description.innerHTML = "Charizard deed vervolgens fire spin, het omringde Blastoise en Blastoise raakte in de war, dus Blastoise kon niet attacken. "
	
	button1.disabled = true;
	
	if (button1.disabled == true) {
		button1.style.backgroundColor = "red"
	}

}

function fireSpin() {
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	document.body.style.backgroundImage = "url('images/Charizard dead.png')"
	
	description.innerHTML = "Charizard deed fire spin, Dat deed niet zo veel, Blastoise deed hydro pump, het was een critical hit, dus Charizard ging dood"
	
	if (button2.disabled == true || button3.disabled == true) {
		button2.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
}

function fireBlast() {
	button2.onclick = megaPunchdead
	
	button3.disabled = true;
	
	description.innerHTML = "Charizard deed fire blast, dat was niet zo effectief, Blastoise deed hydro pump. Charizard overleefde het nog maar net."
	
	if (button3.disabled == true) {
		button3.style.backgroundColor = "red"
	}
}

function fireBlastPotion() {
	inventoryItem.onclick = potionClick
	inventoryItem.style.display = "block";
	button3.disabled = true;
	button3.style.backgroundColor = "red"
	button1.disabled = false;
	button1.onclick = fireSpinPotionDead
	description.innerHTML = "Charizard deed fireblast. dat was niet effectief, Blastoise deed Hydro pump. Charizard overleefde het nog maar net. Je ziet opeens iets glimmen op de grond. Wat ga je doen?"
}

function potionClick() {
	button1.onclick = fireSpinPotion
	inventoryItem.style.display = "none"
	description.innerHTML = "Het is was een full restore! Je Charizard is nu back met full power!"
}

function fireSpinPotion() {
	button1.innerHTML = button1.innerHTML = "Play Again"
	
	button1.onclick = refresh;
	
	button1.disabled = false;
	button3.disabled = true;
	
	document.body.style.backgroundImage = "url('images/Team.png')"
	
	description.innerHTML = "Charizard gooide alles in deze firespin. Blastoise kon niet ontsnappen en werd uiteindelijk uitgeschakeld door de hitte. Je bent Champion van Kanto geworden!"
	
	if (button1.disabled == true || button3.disabled == true) {
		button1.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
	button1.style.backgroundColor = "green"

}

//game over
function gengarDead() {
	title.innerHTML = "Game Over"
	
	document.body.style.backgroundImage = "url('images/Gengar dead.png')"
	
	description.innerHTML = "Je deed Curse, je verloor half je HP om Machamp te cursen en Machamp deed een Earthquake wat super effective was tegen Gengar, Gegnar ging dood "
	
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	if (button2.disabled == true || button3.disabled == true) {
		button2.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
}

function shadowBall() {
	title.innerHTML = "Game Over"
	
	document.body.style.backgroundImage = "url('images/Gengar dead.png')"
	
	description.innerHTML = "Je deed Shadow Ball, Dat had geen effect op Machamp en Machamp deed een Earthquake wat super effective was tegen Gengar, Gengar ging dood "
	
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	if (button2.disabled == true || button3.disabled == true) {
		button2.style.backgroundColor = "red"
		button3.style.backgroundColor = "red"
	}
}

function pikachuDead() {
	title.innerHTML = "Game Over"
	
	document.body.style.backgroundImage = "url('images/Pikachu dead.png')"
	
	description.innerHTML = "Je liet Pikachu Double team gebruiken, zijn ontwijkingsskills ging omhoog, maar Lapras kon alsnog een Ice Beam hitten op Pikachu en ging dood"
	
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	if (button2.disabled == true || button3.disabled == true ) {
	button2.style.backgroundColor = "red"
	button3.style.backgroundColor = "red"
	}
}

function explosion() {
	title.innerHTML = "Game Over"
	
	document.body.style.backgroundImage = "url('images/golem dead.png')"
	
	description.innerHTML = "Golem deed explosion, Dragonite was sterk genoeg om het te overleven, maar aangezien Golem zichzelf letterlijk liet exploderen, ging hij dood"
	
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	button1.onclick = refresh;
	
	if (button2.disabled == true || button3.disabled == true ) {
	button2.style.backgroundColor = "red"
	button3.style.backgroundColor = "red"
	}	
}

function megaPunchdead() {
	
	document.body.style.backgroundImage = "url('images/Charizard dead.png')"
	
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	title.innerHTML = "Game Over"
	
	description.innerHTML = "Charizard deed vervolgens mega punch, dat had ook niet zo veel effect op Blastoise, Blastoise deed een rapid spin en Charizard was dood"
	
	if (button2.disabled == true) {
		button2.style.backgroundColor = "red"
	}
}

function fireSpinPotionDead() {
	document.body.style.backgroundImage = "url('images/Charizard dead.png')"
	
	inventoryItem.style.display = "none"
	button2.disabled = true;
	button3.disabled = true;
	
	button1.innerHTML = button1.InnerHTML = "Try Again"
	
	button1.onclick = refresh;
	
	title.innerHTML = "Game Over"
	
	description.innerHTML = "Charizard deed fire spin, die was niet zo sterk dus Blastoise kon ontsnappen. Blastoise sloeg Charizard door het hele veld heen. En Charizard ging dood"
	
	if (button2.disabled == true) {
		button2.style.backgroundColor = "red"
	}

}
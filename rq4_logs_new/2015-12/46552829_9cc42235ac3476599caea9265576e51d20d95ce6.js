function genererMot(){
	var mots = ['soleil', 'chocolat', 'singe', 'fauteuil'];	
	var mot = mots[Math.floor(Math.random()*mots.length)];

	var emplacementMot = document.getElementById('emplacementMot');
	emplacementMot.innerHTML = mot;
}

genererMot();
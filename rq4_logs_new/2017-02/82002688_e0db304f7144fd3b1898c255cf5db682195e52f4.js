var SolarSystem = (function() {

	var planets = ["mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune"];
	var numPlanetsPeopleLandedOn = 0;
	var spaceships = [];
	var dwarfPlanets = ["pluto"];
	var stars = ["Sun", "Alpha Centauri", "Wolf 395"];
	var age = 0;

	return {
		getPlanets: function() {
			return planets;
		},
		getPlanetsPeopleLandedOn: function() {
			return numPlanetsPeopleLandedOn;
		},
		setPlanetsLandedPeopleOn: function() {
			numPlanetsPeopleLandedOn++;
		},
		getSpaceships: function() {
			return spaceships;
		},
		setSpaceships: function(newSpaceship) {
			spaceships.push(newSpaceship);
		},
		wreckSpaceships: function() {
			spaceships.pop();
		},
		getDwarfPlanets: function() {
			return "the are rejects";
		},
		setDwarfPlanets: function() {
			dwarfPlanets.push(lumpOfRock);
		},
		getStars: function() {
			return stars;
		},
		setStars: function() {
			stars.push(newStar);
		},
		getSolarSystemAge: function() {
			return age;
		},
		setSolarSystemAge: function() {
			age++;
		}

	}
})();

console.log("planets: ", SolarSystem.getPlanets());

console.log("planets people landed on: ", SolarSystem.getPlanetsPeopleLandedOn());

SolarSystem.setSpaceships("Voyager 1");
SolarSystem.setSpaceships("Voyager 2");
SolarSystem.setSpaceships("Gemini");
SolarSystem.setSpaceships("Apollo");
console.log(SolarSystem.getSpaceships());
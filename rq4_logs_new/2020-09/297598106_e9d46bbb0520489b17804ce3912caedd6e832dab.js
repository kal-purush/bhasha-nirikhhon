
// Startup variables
let timeElapsed = 0;
let timeInterval;

// Game data
const maxDistance = 500;
let currentQuestion = 1;
let nQuestions = 2;
let currentScore = 0;
let answers = []; // answers: {answered: Bool, visited: Bool, statue: Statue, score: Int}

let statues = [
  // Name, img src, lat, long
  new Statue("Estatua Mafalda", "images/01-Mafalda.jpeg", 43.362379, -5.850409),
  new Statue("Busto de Manuel Fernández Avello", "images/02-Avello.jpg", 43.362589, -5.851261),
  new Statue("Estatua de Woody Allen", "images/03-WoodyAllen.jpg", 43.363120, -5.849581),
  new Statue("Rufo, el perro de Oviedo", "images/04-Rufo.jpg", 43.364147, -5.851244),
  new Statue("Estatua de Tino Casal", "images/05-TinoCasal.jpg", 43.364252, -5.849895),
  new Statue("Estatua de El Diestro", "images/06-Diestro.jpg", 43.363540, -5.848932),
  new Statue("Culis Monumentalibus", "images/07-Culis.jpg", 43.362776, -5.848237),
  new Statue("Estatua de La Pensadora", "images/08-Pensadora.jpg", 43.362907, -5.847452),
  new Statue("Estatua Amigos", "images/09-Amigos.jpg", 43.363087, -5.845471),
  new Statue("Estatua de La Regenta", "images/10-Regenta.jpg", 43.362183, -5.844498),
  new Statue("Estatua La Maternidad", "images/11-Maternidad.jpg", 43.362002, -5.847802),
  new Statue('"Escultura Esperanza Caminando"', "images/12-Esperanza.jpg", 43.362704, -5.847963),
  new Statue("El regreso de Williams B. Arrensberg", "images/13-Regreso.jpg", 43.362300, -5.845825),
  new Statue('Escultura "La Lechera"', "images/14-Lechera.jpg", 43.360931, -5.843938),
  new Statue("Escultura de Luis Riera Posada", "images/15-LuisRiera.jpg", 43.364868, -5.850678),
]


// Element names
const mapIdPrefix = "map_"
const photoIdPrefix = "photo_"

// Element variables
let maps = [];
let images = [];

// Google map objects
let gMaps = [];

// Map settings and vars
const oviedo_centro = new google.maps.LatLng(43.361585, -5.850545);

const mapInitialOptions = {
  center: oviedo_centro,
  zoom: 16,
  minZoom: 15,
  mapTypeId: google.maps.MapTypeId.SATELLITE,
  disableDefaultUI: true,
};

const mapFinalOptions = { // Remove interactivity on question answered
  zoomControl: false,
  gestureHandling: 'none'
};

const infowindow = new google.maps.InfoWindow();

// Limit bounds
const allowedBounds = new google.maps.LatLngBounds(
  new google.maps.LatLng(43.355749, -5.860596),
  new google.maps.LatLng(43.369993, -5.839336)
);
let lastValidCenters = [];

// Circle settings
const circleRadius = 60;
const circleOptions = {
  strokeColor: '#0000FF',
  strokeOpacity: 0.8,
  strokeWeight: 2,
  fillColor: '#00FF00',
  fillOpacity: 0.15,
};

// Polyline settings
const lineOptions = {
  geodesic: true,
  strokeColor: "#FF0000",
  strokeOpacity: 1.0,
  strokeWeight: 2,
};


// CODE

window.addEventListener("load", () => {

  setupGame();
  setupUI();
  loadGMap(0);

})

function setupGame() {
  if (nQuestions > statues.length) nQuestions = statues.length;

  // Shuffle questions and create maps and images
  const questions = [...statues] // Clone of statues array

  for (let i = 0; i < nQuestions; i++) {
    const index = randomIntFromInterval(0, questions.length - 1);

    answers.push({
      answered: false,
      visited: false,
      statue: questions[index],
      score: 0
    });

    answers[0].visited = true;

    questions.splice(index, 1);
  }

}

function setupUI() {
  // Initialize counter
  let timeElement = document.getElementById("time-current");
  timeInterval = setInterval(() => {
    timeElapsed += 1;
    timeElement.innerText = new Date(timeElapsed * 1000).toISOString().substr(11, 8);
  }, 1000)

  // Total questions
  document.getElementById("total").innerText = nQuestions;

  // Add map && photo elements as needed:
  const mapContainer = document.querySelector("section.map.game-element");
  const photoContainer = document.getElementById("photo");

  for (let i = 0; i < nQuestions; i++) {

    if (i == 0) {
      mapContainer.innerHTML += `<div id="map_${i + 1}">Map ${i + 1}</div>`
      photoContainer.innerHTML += `<img id="photo_${i + 1}" alt="${answers[i].statue.name}" src="${answers[i].statue.img}">`
    }
    else {
      // Add maps with correct ID
      mapContainer.innerHTML += `<div id="map_${i + 1}" class="hidden">Map ${i + 1}</div>`

      // Add images with correct src
      photoContainer.innerHTML += `<img id="photo_${i + 1}" class="hidden" alt="${answers[i].statue.name}" src="${answers[i].statue.img}">`
    }
  }

  // Initialize elements
  maps = document.querySelectorAll(`[id^=${mapIdPrefix}]`)
  photos = document.querySelectorAll(`[id^=${photoIdPrefix}]`)

  // Set arrow buttons functionality:
  document.getElementById("prev").addEventListener("click", () => changeQuestion(-1));
  document.getElementById("next").addEventListener("click", () => changeQuestion());

  // Check answer functionality
  document.getElementById("check").addEventListener("click", checkAnswer)

  const finishBtn = document.getElementById("finish");

  finishBtn.disabled = true;
  finishBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    e.preventDefault();

    if (!answers.every(a => a.answered === true)) {
      alert("¡Responde a todas las preguntas antes de terminar!");
    }
    else {
      const scores = [];
      answers.forEach(a => scores.push(a.score));
      window.location.href = `results.html?time=${timeElapsed}&scores=${JSON.stringify(scores)}&total=${currentScore}`;
    }
  })

  changeQuestion(1);
}


// If prev, change to the question before, else, to the next question
function changeQuestion(prev) {
  if (prev) {
    if (currentQuestion > 1) setCurrentQuestion(currentQuestion - 1)
  }
  else if (currentQuestion < nQuestions) setCurrentQuestion(currentQuestion + 1)

  // Change question text
  document.getElementById("current").innerText = currentQuestion;

  // Disable buttons as required
  if (currentQuestion >= nQuestions) document.getElementById("next").disabled = true;
  else if (currentQuestion <= 1) document.getElementById("prev").disabled = true;
  else {
    document.getElementById("next").disabled = false;
    document.getElementById("prev").disabled = false;
  }

  const checkBtn = document.getElementById("check");
  // Disable check answer if question was answered
  if (answers[currentQuestion - 1] && answers[currentQuestion - 1].answered) {
    checkBtn.disabled = true;
    checkBtn.innerText = "Puntos: " + answers[currentQuestion - 1].score;
  }
  else {
    checkBtn.disabled = false;
    checkBtn.innerText = "Comprobar respuesta";
  }

  // Show maps as required
  for (map of maps) {
    if (compareStrings(map.id, mapIdPrefix + currentQuestion.toString())) {
      map.classList.remove("hidden");
    }
    else {
      map.classList.add("hidden");
    }
  }

  // Show images as required
  for (photo of photos) {
    if (compareStrings(photo.id.toString(), photoIdPrefix + currentQuestion.toString())) {
      photo.classList.remove("hidden");
    }
    else {
      photo.classList.add("hidden");
    }
  }

  if (currentQuestion == answers.length) {
    document.getElementById("finish").disabled = false;
  }
  else {
    document.getElementById("finish").disabled = true;
  }

}

function setCurrentQuestion(n) {
  if (n > 0 || n <= nQuestions) {
    currentQuestion = n;

    if (!answers[n - 1].visited) loadGMap();
    answers[n - 1].visited = true;
  }
}


function checkAnswer() {

  const currentMap = gMaps[currentQuestion - 1];

  // The user must answer first
  if (!currentMap.answerMarker) {
    alert("¡Coloca tu respuesta primero haciendo click en el mapa!");
    return
  }
  // Disable button for this question for the rest of the game
  const checkBtn = document.getElementById("check");
  checkBtn.disabled = true;


  // Show correct answer
  answers[currentQuestion - 1].answered = true;
  currentMap.correctMarker.setVisible(true);

  // Limit map functionality
  currentMap.setOptions({ ...mapInitialOptions, ...mapFinalOptions, zoom: 1 });
  google.maps.event.clearInstanceListeners(currentMap);

  // Move map to answer
  currentMap.panTo(currentMap.correctMarker.getPosition());

  const line = drawAnswer();

  computeScore();

  checkBtn.innerText = "Puntos: " + answers[currentQuestion - 1].score;

}

// On check answer: draw circles around the target and straight line from answer to target
function drawAnswer() {
  const currentMap = gMaps[currentQuestion - 1];

  // Proximity circles
  for (let i = circleRadius; i <= circleRadius * 4; i += circleRadius) {
    new google.maps.Circle({
      ...circleOptions,
      map: currentMap,
      radius: i,
      center: currentMap.correctMarker.getPosition()
    });
  }

  // Line answer-target
  const answerCoordinates = currentMap.answerMarker.getPosition();
  const correctCoordinates = currentMap.correctMarker.getPosition();
  new google.maps.Polyline({
    ...lineOptions,
    map: currentMap,
    path: [
      { lat: answerCoordinates.lat(), lng: answerCoordinates.lng() },
      { lat: correctCoordinates.lat(), lng: correctCoordinates.lng() },
    ]
  });
}

function computeScore() {
  const currentMap = gMaps[currentQuestion - 1];
  // Update score
  const answerCoordinates = currentMap.answerMarker.getPosition();
  const correctCoordinates = currentMap.correctMarker.getPosition();
  const distanceToTarget = Math.floor(google.maps.geometry.spherical.computeDistanceBetween(answerCoordinates, correctCoordinates));


  const newScore = Math.max(0, maxDistance - distanceToTarget);
  answers[currentQuestion - 1].score = newScore;
  currentScore += newScore;

  // Update UI
  document.getElementById("score-current").innerText = currentScore;

  infowindow.setContent(`Distancia al objetivo: <b>${distanceToTarget}m</b>`)
  infowindow.open(currentMap, currentMap.correctMarker);
}

// GOOGLE MAPS FUNCTIONALITY

function loadGMap(n) {
  const currentMap = n || currentQuestion - 1;
  try {

    // Create map
    const gMap = new google.maps.Map(maps[currentMap], mapInitialOptions);
    gMaps.push(gMap);


    // Limit coordinates
    google.maps.event.addListener(gMap, 'center_changed', () => {
      if (allowedBounds.contains(gMap.getCenter())) {
        // still within valid bounds, so save the last valid position
        lastValidCenters[currentMap] = gMap.getCenter();
        return;
      }

      // not valid anymore => return to last valid position
      if (lastValidCenters[currentMap]) gMap.panTo(lastValidCenters[currentMap]);
    });

    // Set the correct answer marker and hide it
    gMap.correctMarker = new google.maps.Marker({
      title: answers[currentMap].statue.title,
      position: new google.maps.LatLng(answers[currentMap].statue.lat, answers[currentMap].statue.long),
      icon: answerIcon,
      map: gMap
    });

    google.maps.event.addListener(gMap.correctMarker, "click", (evt) => {
      infowindow.setContent(`${answers[currentMap].statue.name}<br/>[${evt.latLng.lat().toFixed(3)}, ${evt.latLng.lng().toFixed(3)}]`)
      infowindow.open(gMap, gMap.correctMarker);
    });

    gMap.correctMarker.setVisible(false);

    // Listener to place marker on click
    google.maps.event.addListener(gMap, 'click', (event) => {

      // Remove user marker if one already exists
      if (gMap.answerMarker) gMap.answerMarker.setMap(null);

      // Place new marker
      gMap.answerMarker = new google.maps.Marker({
        title: "Respuesta del usuario",
        icon: myIcon,
        position: event.latLng,
        map: gMap
      });


      google.maps.event.addListener(gMap.answerMarker, "click", (evt) => {
        infowindow.setContent(`Respuesta del usuario<br/>[${evt.latLng.lat().toFixed(3)}, ${evt.latLng.lng().toFixed(3)}]`);
        infowindow.open(gMap, gMap.answerMarker);
      });


    });

  }
  catch (e) {
    maps[currentMap].innerText = "No se pudo cargar el mapa, prueba a refrescar la página.";
  }
}

// HELPERS
function randomIntFromInterval(min, max) { // min and max included
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function compareStrings(str1, str2) {
  if (str1.localeCompare(str2) == 0) return true;
  return false;
}

const myIcon = {
  url: "http://maps.google.com/mapfiles/ms/icons/red-dot.png",
};

const answerIcon = {
  url: "http://maps.google.com/mapfiles/ms/icons/blue-dot.png"
};
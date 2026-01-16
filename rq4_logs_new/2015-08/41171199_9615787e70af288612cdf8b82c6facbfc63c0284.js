
var HTMLheaderName = '<h1 id="name">%data%</h1>';
var HTMLheaderRole = '<span class="white-text">%data%</span><hr/>';

var HTMLcontactGeneric = '<li class="flex-item"><span class="orange-text">%contact%</span><span class="white-text">%data%</span></li>';

var HTMLemail = '<li class="flex-item"><span class="orange-text">Email</span><span class="white-text">%data%</span></li>';

var HTMLgithub = '<li class="flex-item"><span class="orange-text">GitHub</span><span><a href="https://github.com/Shanmathi1" class="link">%data%</a></span></li>';
var HTMLlinkedin = '<li class="flex-item"><span class="orange-text">LinkedIn</span><span><a href="https://www.linkedin.com/in/shanmathimk" class="link">%data%</a></span></li>';
var HTMLlocation = '<li class="flex-item"><span class="orange-text">Location</span><span class="white-text">%data%</span></li>';

var HTMLbioPic = '<img src="%data%" class="biopic">';
var HTMLwelcomeMsg = '<span class="welcome-message">%data%</span>';

var HTMLskillsStart = '<h3 id="skills-h3">Skills at a Glance:</h3><ul id="skills" class="flex-box"></ul>';
var HTMLskills = '<li class="flex-item"><span class="white-text">%data%</span></li>';

var HTMLworkStart = '<div class="work-entry"></div>';
var HTMLworkEmployer = "<a class='no-click' href='#'>%data%";
var HTMLworkTitle = ' - %data%</a>';
var HTMLworkDates = '<div class="date-text">%data%</div>';
var HTMLworkLocation = '<div class="location-text">%data%</div>';
var HTMLworkDescription = "<p class='desc'><br>%data%</p><br>";

var HTMLprojectStart = '<div class="project-entry"></div>';
var HTMLprojectTitle = "<a href='%url%' target='_blank'>%data%</a>";
var HTMLprojectDates = '<div class="date-text">%data%</div>';
var HTMLprojectDescription = '<p><br>%data%</p>';
var HTMLprojectImage = "<img class='project-img' src='%data%'><hr class='section-hr'>";;

var HTMLschoolStart = '<div class="education-entry"></div>';
var HTMLschoolName = "<br><a class='no-click' href='#'>%data%";
var HTMLschoolDegree = ' -- %data%</a>';
var HTMLschoolDates = '<div class="date-text">%data%</div>';
var HTMLschoolLocation = '<div class="location-text">%data%</div>';
var HTMLschoolMajor = '<em><br>Major: %data%</em>';
var HTMLschoolGPA = '<em><br>GPA: %data%</em>';



var internationalizeButton = '<button>Internationalize</button>';
var googleMap = '<div id="map"></div>';



var map;    // declares a global map variable



function initializeMap() {

  var locations;

  var mapOptions = {
    disableDefaultUI: true
  };

  
  map = new google.maps.Map(document.querySelector('#map'), mapOptions);


  
  function locationFinder() {

  
    var locations = [];

    locations.push(bio.contacts.location);

    for (var school in education.schools) {
      locations.push(education.schools[school].location);
    }

 
    for (var job in work.jobs) {
      locations.push(work.jobs[job].location);
    }

    return locations;
  }

  
  function createMapMarker(placeData) {

    
    var lat = placeData.geometry.location.lat();  // latitude from the place service
    var lon = placeData.geometry.location.lng();  // longitude from the place service
    var name = placeData.formatted_address;   // name of the place from the place service
    var bounds = window.mapBounds;            // current boundaries of the map window

    
    var marker = new google.maps.Marker({
      map: map,
      position: placeData.geometry.location,
      title: name
    });

    
    var infoWindow = new google.maps.InfoWindow({
      content: name
    });

   

    bounds.extend(new google.maps.LatLng(lat, lon));
  
    map.fitBounds(bounds);
    
    map.setCenter(bounds.getCenter());
  }

  
  function callback(results, status) {
    if (status == google.maps.places.PlacesServiceStatus.OK) {
      createMapMarker(results[0]);
    }
  }

  
  function pinPoster(locations) {

 
    var service = new google.maps.places.PlacesService(map);

    for (var place in locations) {

      // the search request object
      var request = {
        query: locations[place]
      };

     
      service.textSearch(request, callback);
    }
  }

  // Sets the boundaries of the map based on pin locations
  window.mapBounds = new google.maps.LatLngBounds();

  // locations is an array of location strings returned from locationFinder()
  locations = locationFinder();

  // pinPoster(locations) creates pins on the map for each location in
  // the locations array
  pinPoster(locations);

}



// Calls the initializeMap() function when the page loads
window.addEventListener('load', initializeMap);

// Vanilla JS way to listen for resizing of the window
// and adjust map bounds
window.addEventListener('resize', function(e) {
  //Make sure the map bounds get updated on page resize
 map.fitBounds(mapBounds);
});
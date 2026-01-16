
// Parse JSON from blueprint

// Create Room Objects from the JSON 

// TODO: add testJSON
//var testJSON = {};

// Define a room object
// Room objects: attr= name, dimension, floor 
function Room(name, width, height, floor){

   this.name = name;
   this.width = width;
   this.height = height;
   this.floor = floor;
}

//MAKING SAMPLE ROOM LIST: 
var sampleRoomList = [];
sampleRoomList.push(new Room("Master Bedroom", 10, 20, 1));
sampleRoomList.push(new Room("Bedroom", 10, 10, 1));
sampleRoomList.push(new Room("Kitchen", 10, 20, 1));
sampleRoomList.push(new Room("Living Room", 10, 20, 1));
sampleRoomList.push(new Room("Dining Room", 10, 15, 1));
sampleRoomList.push(new Room("BathroomF", 5, 8, 1));
sampleRoomList.push(new Room("BathroomH", 5, 4, 1));

console.log(sampleRoomList);

// Define a cluster
function Cluster(roomList, width, height){

   this.roomList = roomList;
   this.width = width;
   this.height = height;
   this.horizontal = true;
}

// Add methods like this.  All Person objects will be able to invoke this
// Person.prototype.speak = function(){
//     alert("Howdy, my name is" + this.name);
// };

//returns a list of Room objects
// use: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/parse 
function JSONtoRoomList(json) {

}

// input: a list of Room objects
// output: a set of 4 clusters, where each cluster is a rectangle which holds a set of rooms

 
function shuffle(array) {
	// source: http://stackoverflow.com/questions/2450954/how-to-randomize-shuffle-a-javascript-array
  var currentIndex = array.length, temporaryValue, randomIndex ;

  // While there remain elements to shuffle...
  while (0 !== currentIndex) {

    // Pick a remaining element...
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex -= 1;

    // And swap it with the current element.
    temporaryValue = array[currentIndex];
    array[currentIndex] = array[randomIndex];
    array[randomIndex] = temporaryValue;
  }

  return array;
}

function groupRooms(roomList) {
	// shuffle array
	roomList = shuffle(roomList);

	// set up clusters:
	//var clusterDict = '{"0": [], "1": [], "2": [], "3": []}';

  var groupList = [[],[],[],[]];

	// match rooms into four clusters
	for (i=0; i<roomList.length; i++) {
		// var clusterIndex = (i % 4); 
		// var clusterDictObj = JSON.parse(clusterDict);
		// clusterDictObj[clusterIndex].push(roomList[i]);
		// clusterDict = JSON.stringify(clusterDictObj);

    var groupIndex = (i % 4); 
    groupList[groupIndex].push(roomList[i]);
	}


  console.log(groupList);

  return groupList;

	
	// determine flush dimensions
	// return list of clusters (each a list of rooms)


}

var groupList = groupRooms(sampleRoomList);
var clusterList = createClusterMap(groupList);

function createClusterMap(groupList) {
  var clusterList = [];

  for (i=0; i<groupList.length; i++) {
    var totalWidth = 0;
    var totalHeight = 0;
    var roomList = groupList[i];
    for (roomIndex = 0; roomIndex<roomList.length; roomIndex++) {
      var room = roomList[roomIndex];
      totalWidth = Math.max(room.width);
      totalHeight += room.height;
    }
    var cluster = new Cluster(roomList, totalWidth, totalHeight);
    clusterList.push(cluster);
  }

  console.log(clusterList);

  function compare(a,b) {
    return (b.width*b.height) - (a.width*a.height);
  };

  clusterList.sort(compare);

  return clusterList;

}

//gets random order of corners 1, 2, 3, 4 to place clusters in
function getRoomPlacementOrder() {
  return shuffle([1,2,3,4]);
}



























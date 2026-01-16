var Context = require("Modules/Context");

var hike = this.Parameter;

var name = hike.map((current) => { return current.name; });
var location = hike.map((current) => { return current.location; });
var distance = hike.map((current) => { return current.distance; });
var rating = hike.map((current) => { return current.rating; });
var comments = hike.map((current) => { return current.comments; });

function cancel() {
	router.goBack();
}

function save() {
	Context.saveHike(name.value, location.value, distance.value, rating.value, comments.value);

	// Clear values
	name.value = null;
	location.value = null;
	distance.value = null;
	rating.value = null;
	comments.value = null;
	router.goBack();
}

module.exports = {
	name: name,
	location: location,
	distance: distance,
	rating: rating,
	comments: comments,

	cancel: cancel,
	save: save
};
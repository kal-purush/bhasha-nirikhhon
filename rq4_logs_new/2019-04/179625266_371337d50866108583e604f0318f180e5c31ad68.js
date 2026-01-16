// Initialize Firebase
var config = {
    apiKey: "AIzaSyDXoWWWGL5Fd_1U0AJnbXeR2Ur0VoM54RY",
    authDomain: "train-scheduler-fb382.firebaseapp.com",
    databaseURL: "https://train-scheduler-fb382.firebaseio.com",
    projectId: "train-scheduler-fb382",
    storageBucket: "train-scheduler-fb382.appspot.com",
    messagingSenderId: "181508120277"
};
firebase.initializeApp(config);

// Create a variable to reference the database
var database = firebase.database();

// Initial Values
var name = "";
var destination = "";
var initialTime = 0;
var frequency = "";

// Capture Button Click
$("#add-train").on("click", function (event) {
    // Don't refresh the page!
    event.preventDefault();


    // Storing and retrieving the most recent user.
    // Don't forget to provide initial data to your Firebase database.
    name = $("#name-input").val().trim();
    destination = $("#destination-input").val().trim();
    initialTime = $("#initial-time-input").val().trim();
    frequency = $("#frequency-input").val().trim();

    // Creates local "temporary" object for holding train data
    var newTrain = {
        name: name,
        destination: destination,
        initialTime: initialTime,
        frequency: frequency
    };

    // Upload train data to the database
    database.ref().push(newTrain);

    alert("Train successfully added");

    // Clears all of the text-boxes
    $("#name-input").val("");
    $("#destination-input").val("");
    $("#initial-time-input").val("");
    $("#frequency-input").val("");
});

// Firebase event for adding train to the database and a row in the html when the user adds an entry
database.ref().on("child_added", function (childSnapshot) {
    console.log(childSnapshot.val());

    // store everything into a variables
    let trainName = childSnapshot.val().name;
    let trainDest = childSnapshot.val().destination;
    let trainInitTime = childSnapshot.val().initialTime;
    let trainFreq = childSnapshot.val().frequency;

    // Train Info

    var timeFormat = "H:mm A"
    var convertedTime = moment(trainInitTime, timeFormat);
    // console.log("converted time w format", convertedTime.format("H:mm A"))
    // console.log(convertedTime);

    // var trainInitTimePretty = moment.unix(trainInitTime).format("H:mm");

    // Calculate time from initial start to now
    var trainAway = moment().diff(moment(trainInitTime, "X"), "minutes")
    console.log("trainAway is", trainAway);


    var a = moment(trainInitTime, "H:mm");
    var b = moment().format("H:mm");

    // Calculate minutes away
    // var differece = 


    // if train hasn't started
    if (moment(b, "hh:mma").isBefore(moment(a, "hh:mma"))) {
        // nextArr = convertedTime.format("hh:mm A");
        // console.log(convertedTime.format("hh:mm A"))

        console.log("THEORY", convertedTime.diff(moment(), "minutes"));
        var diff = convertedTime.diff(moment(), "minutes");
        console.log(diff);
        console.log("PART 2", moment(convertedTime).add(diff, 'minutes'))


        var minAway = convertedTime.diff(moment(), "minutes") + 1;

    } else {
        // Calculate the Minutes Away
        a = moment(trainInitTime, "H:m");
        b = moment().format("H:m");
        var calc = moment(moment(b, "H:m").diff(moment(a, "H:m"))).format("mm") % trainFreq;
        var minAway = trainFreq - calc

        // Calculate the next arrival
        var duration = moment.duration({ 'minutes': minAway })
        var nextArrNoFormat = moment().add(duration);
        var nextArr = moment(nextArrNoFormat).format("h:mm A");
        console.log("look here friend", moment().diff(convertedTime, "minutes"));
        // var nextArr = moment().diff(convertedTime, "minutes");
    }

    // Create the new row
    var newRow = $("<tr>").append(
        $("<td>").text(trainName),
        $("<td>").text(trainDest),
        $("<td>").text(trainFreq),
        $("<td>").text(nextArr),
        $("<td>").text(minAway),
    );

    // Apprend the new row to the table
    $("#train-table > tbody").append(newRow);
})
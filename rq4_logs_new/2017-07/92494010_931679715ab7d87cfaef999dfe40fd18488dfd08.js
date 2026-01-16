function loadVoices() {
	// Fetch the available voices.
	var voices = window.speechSynthesis.getVoices();

	// Loop through each of the voices.
	voices.forEach(function (voice, i) {
		// Create a new option element.
		console.log(voices);

		var option = document.createElement('option');

		// Set the options value and text.
		option.value = voice.name;
		option.innerHTML = voice.name;
		// Add the option to the voice selector.
		$("#voiceSelect").appendChild(option);
	});
}

var recognition;
nlp = window.nlp_compromise;
var accessToken = "872500f5983f46568df07d5ab0305eed";
var baseUrl = "https://api.api.ai/v1/";
var messages = [], //array that hold the record of each string in chat
lastUserMessage = "", //keeps track of the most recent input string from the user
botMessage = "", //var keeps track of what the chatbot is going to say
botName = 'Dr. Who'; //name of the chatbot

function startRecognition() {
	recognition = new webkitSpeechRecognition();
	recognition.onstart = function (event) {
		updateRec();
	};
	recognition.onresult = function (event) {
		var text = "";
		for (var i = event.resultIndex; i < event.results.length; ++i) {
			text += event.results[i][0].transcript;
		}
		setInput(text);
		stopRecognition();
	};
	recognition.onend = function () {
		stopRecognition();
	};
	recognition.lang = "en-US";
	recognition.start();
}

function stopRecognition() {
	if (recognition) {
		recognition.stop();
		recognition = null;
	}
	updateRec();
}

function switchRecognition() {
	if (recognition) {
		stopRecognition();
	} else {
		startRecognition();
	}
}

function setInput(text) {
	$("#chatbox").val(text);
	// send();
	newEntry();
}

function updateRec() {
	// $("#rec").text(recognition ? "Stop" : "Speak");
	image_url = (recognition ? "assets/image/microphone-309680_960_720.png" : "assets/image/microphone-2104091_960_720.png")
	$("#rec").css('background-image', 'url(' + image_url + ')');
}

function send() {
	//var text = $("#chatbox").val();
	var text = lastUserMessage;
	$.ajax({
		type: "POST",
		url: baseUrl + "query?v=20150910",
		contentType: "application/json; charset=utf-8",
		dataType: "json",
		headers: {
			"Authorization": "Bearer " + accessToken
		},
		data: JSON.stringify({
			query: text,
			lang: "en",
			sessionId: "241187"
		}),

		success: function (data) {
			setResponse(JSON.stringify(data, undefined, 2));
			setAudioResponse(data);

		},
		error: function () {
			setResponse("Internal Server Error");
		}
	});
	setResponse("Loading...");
}

function setResponse(val) {
	$("#response").text(val);
}

function setAudioResponse(val) {
	 // $("#response").text(val);
	if (val.result) {
		var say = "";
		say = val.result.fulfillment.messages;
		// botMessage = say
		for (var j = 0; j < say.length; j++) {
			botMessage = say[j].speech;

			messages.push("<b>" + botName + ":</b> " + botMessage);
			for (var i = 1; i < 11; i++) {
				if (messages[messages.length - i])
					$("#chatlog" + i).html(messages[messages.length - i]);
			}
			synth = window.speechSynthesis;
			var utterThis = new SpeechSynthesisUtterance(botMessage);
			utterThis.lang = "en-US";
			// utterThis.lang = "hi-IN";
			utterThis.name = "Google US English";
			synth.speak(utterThis);
		}
	}
}
//this runs each time enter is pressed.
//It controls the overall input and output
function newEntry() {
	//if the message from the user isn't empty then run
	if ($("#chatbox").val() != "") {
		//pulls the value from the chatbox ands sets it to lastUserMessage
		lastUserMessage = $("#chatbox").val();
		//sets the chat box to be clear
		$("#chatbox").val("");
		//adds the value of the chatbox to the array messages
		messages.push("<b>Me: </b>" + lastUserMessage);
		//Speech(lastUserMessage);  //says what the user typed outloud
		//sets the variable botMessage in response to lastUserMessage
		send();
		// botMessage = '';
		//add the chatbot's name and message to the array messages
		//messages.push("<b>" + botName + ":</b> " + botMessage);
		// says the message using the text to speech function written below
		//outputs the last few array elements of messages to html
		for (var i = 1; i < 11; i++) {
			if (messages[messages.length - i])
				$("#chatlog" + i).html(messages[messages.length - i]);
		}
	}
}

//clears the placeholder text ion the chatbox
//this function is set to run when the users brings focus to the chatbox, by clicking on it
$("#chatbox").focus(function () {
	$(this).attr("placeholder", "");
});
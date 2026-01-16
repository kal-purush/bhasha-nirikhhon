const btnrecord = document.getElementById("start");
const btnstop = document.getElementById("stop");
const btnsend = document.getElementById("send");
const trash = document.getElementById("trash");
const listObject = document.getElementById("recordings");
var myRecorder = {
    objects: {
        context: null,
        stream: null,
        recorder: null
    }
};
// Prepare the recordings list
const recording = () => {
    if (myRecorder.objects.context == null) {
        myRecorder.objects.context = new(
            window.AudioContext || window.webkitAudioContext
        );
    }
    const options = {
        audio: true,
        video: false
    };
    navigator.mediaDevices.getUserMedia(options).then(function (stream) {
        btnstop.style.display = "block";
        btnrecord.style.display = "none";
        myRecorder.objects.stream = stream;
        myRecorder.objects.recorder = new Recorder(
            myRecorder.objects.context.createMediaStreamSource(stream), {
                numChannels: 1
            }
        );
        myRecorder.objects.recorder.record();
    }).catch(function (err) {});
}
const stoprecording = () => {
    btnstop.style.display = "none";
    btnsend.style.display = "block";
    trash.style.display = "block";
    if (null !== myRecorder.objects.stream) {
        myRecorder.objects.stream.getAudioTracks()[0].stop();
    }
    if (null !== myRecorder.objects.recorder) {
        myRecorder.objects.recorder.stop();
        // Export the WAV file
        myRecorder.objects.recorder.exportWAV(function (blob) {
            const url = (window.URL || window.webkitURL)
                .createObjectURL(blob);
            // Prepare the playback
            const node = document.createElement("audio");
            var att = document.createAttribute("src"); // Create a "class" attribute
            att.value = url;
            var att2 = document.createAttribute("controls"); // Create a "class" attribute
            node.setAttributeNode(att2);
            node.setAttributeNode(att);
            listObject.appendChild(node);
        });
    }
}

const submit = () => {
    location.reload();
}


const deleteaudio = () => {
    location.reload();
}
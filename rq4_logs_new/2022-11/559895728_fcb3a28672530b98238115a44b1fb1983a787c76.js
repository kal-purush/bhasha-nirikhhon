let video = document.querySelector("#video");
let camera_button = document.querySelector("#start-camera");
let start_button = document.querySelector("#start-record");
let stop_button = document.querySelector("#stop-record");
let download_link = document.querySelector("#download-video");
let result = document.getElementById('result')

let camera_stream = null
let media_recorder = null
let blobs_recorded = []
let streams =[]
let res

camera_button.addEventListener("click", async function(){
    camera_stream = await navigator.mediaDevices.getUserMedia({video:true})
    video.srcObject = camera_stream
})

function form_handler(event) {
    event.preventDefault();
}

start_button.addEventListener('click', function(){
    media_recorder = new MediaRecorder(camera_stream, {mimeType:'video/webm'})

    media_recorder.addEventListener('dataavailable', function(e){
        blobs_recorded.push(e.data)
    })

    media_recorder.addEventListener('stop', function(){
        
        var xhr = new XMLHttpRequest();

    
        let video_local = URL.createObjectURL(new Blob(blobs_recorded,{type:'video/webm'}));

        let obj={
            name : Date.now()+"Recording",
            url:video_local
        }
        streams.push(obj)
        console.log(blobs_recorded)

        streams.forEach(stream =>{

            res = `
            <form method="POST">
            <input type="text" name="url" id="url" value="${stream.url}">
           
            </form>
            <table>
            <tr>
            <td>${stream.name}</td>
            <td><a download="${Date.now() + "recording.webm"}" href="${stream.url}">Download Video</a></td>
            </tr>
            </table>
            `
            $("#result").append(res)
        });
        
       

        var xhr = new XMLHttpRequest();
        var params = obj.url;

        xhr.open('POST','/webcam'+"?"+"arg1"+"="+params,true);
        xhr.setRequestHeader("Content-type", "application/json");
        xhr.onload = function(){};
        var fd = new FormData(document.querySelector('form'));
    
        //xhr.send(JSON.stringify(obj));
        xhr.send(fd)

        media_recorder=null
        blobs_recorded=[]
    
    })
    media_recorder.start(1000);
})

stop_button.addEventListener('click',function(){
    media_recorder.stop();
})
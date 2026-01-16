const socket = new WebSocket('ws://localhost:8080');

document.getElementById("creating_room_option").addEventListener("click", creating_room_option);
document.getElementById("login_room_option").addEventListener("click", login_to_room_option);

document.getElementById("creat_room_button").addEventListener("click", creating_room);
document.getElementById("login_button").addEventListener("click", login_to_room);

socket.addEventListener('open', function(event) {
    socket.send("get_avatar||1");
});

socket.addEventListener('message', function(event){
    let message = event.data.toString();
    console.log(message);
    let message_split = message.split("||");

    if(message_split[0] == "login_accepted"){
        sessionStorage.setItem("player_name", message_split[1]);
        sessionStorage.setItem("player_gender", message_split[2]);
        sessionStorage.setItem("room_id", message_split[3]);
        sessionStorage.setItem("player_id", message_split[4]);
        sessionStorage.setItem("avatar", message_split[5]);
        window.location.href = "playing_lobby.html";
    }

    if(message_split[0] == "avatar"){
        document.getElementById("avatar_table").innerHTML = message_split[1];
        
        // Add event listeners for the dynamically inserted avatar images
        document.querySelectorAll('.img-thumbnail').forEach(img => {
            img.addEventListener('click', function() {
                document.getElementById("avatar_path").value=this.src
                //console.log("Avatar selected!"+ " "+this.src+ " "+document.getElementById("avatar_path").value);
            });
        });
    }
});

function creating_room_option(){
    document.getElementById("login").style.display = "none";
    document.getElementById("creating").style.display = "block";
}

function login_to_room_option(){
    document.getElementById("creating").style.display = "none";
    document.getElementById("login").style.display = "block";
}

function login_to_room(){
    let player_name = document.getElementById("player_name").value;
    let player_gender = document.getElementById("player_gender").value;
    let Room_id = document.getElementById("Room_id").value;
    let avatar = document.getElementById("avatar_path").value;
   
    let message = "login||" + player_name + "||" + player_gender + "||" + Room_id+"||"+avatar
    socket.send(message);
}

function creating_room(){
    console.log("Room Creating...");
    let player_name = document.getElementById("player_name_creating_room").value;
    let player_gender = document.getElementById("player_gender_creating_room").value;
    let avatar = document.getElementById("avatar_path").value;
    
    let room_id = Math.ceil(Math.random() * 1000 + 1);
    let message = "creating_room||" + player_name + "||" + player_gender + "||" + room_id+"||"+avatar;
    console.log(message)
    socket.send(message);
}
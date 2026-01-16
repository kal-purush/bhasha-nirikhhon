//signal to load a file into main.php via php
function loadEvent(event){
	var file = event.target.id.split("-",2)[1];
    window.location.href = "main.php?file=" + file;
}
//signal to download specified file via php
function downloadEvent(event){
	var file = event.target.id.split("-",2)[1];
    window.location.href = "files.php?download=" + file;
}
//signal to delete a file via php
function deleteEvent(event){
	var file = event.target.id.split("-",2)[1];
	window.location.href = "files.php?delete=" + file;
}

//attach all functions to html elements
window.onload = function() {
	//each of these loops goes through all of the files and their buttons
	//and assigns them to the corresponding events to link off to php
	var loads = document.getElementsByName("load");
    for(var i = 0; i < loads.length; i++){
        loads[i].onclick = loadEvent;
    }
	var downloads = document.getElementsByName("download");
    for(var i = 0; i < downloads.length; i++){
        downloads[i].onclick = downloadEvent;
    }
	var deletes = document.getElementsByName("delete");
    for(var i = 0; i < downloads.length; i++){
        deletes[i].onclick = deleteEvent;
    }
}
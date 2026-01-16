function changeColor(){
    var newColor = document.getElementById("studentPageColor").value;
    changeBackgroundColor(newColor);
}

function clearAllFromSchools(){
    var checkboxArray = document.getElementsByName("from-check");
    for(i=0; i<checkboxArray.length; i++){
        if(checkboxArray[i].checked == true){
            checkboxArray[i].checked = false;
        }
    }
}

function clearAllToSchools(){
    var checkboxArray = document.getElementsByName("to-check");
    for(i=0; i<checkboxArray.length; i++){
        if(checkboxArray[i].checked == true){
            checkboxArray[i].checked = false;
        }
    }
}
$( document ).ready(function() {

  	$("div.cursor").click( function(){
  		$(this).next("div.custom_hidden").toggle();
  	});
});

var validation = new RegExp("[0-9]+$");
var multipliers = [2.5,3.5,2.0,2.0];


function calculatePoints() {
    var inputValues = document.getElementsByClassName( 'percentInput' );
    var errorNotice = document.getElementById('error');
    for(var i=0;i<4;i++) {
        if(inputValues[i].value.length>3||!validation.test(inputValues[i].value)||inputValues[i].value>100) {
            errorNotice.innerHTML = "Lūdzu ievadi pareizus datus :)";
            return;
        }
    }
    errorNotice.innerHTML = "";


    var totalPoints = 0;
    for(var i=0;i<4;i++){
        totalPoints +=inputValues[i].value*multipliers[i];
    }
    document.getElementById('totalPoints').innerHTML = totalPoints;
}
   /* var array=document.getElementsByName('qty');
    var totalPoints=0;
    var multipliers = [2.5,3.5,2.0,2.0];
    document.getElementById('totalPoints').value=totalPoints;
    alert(array[i].value+'COOL');
    for(var i=0;i<4;i++) {
        if (!/^[0-9]+$/.test(array[i].value)) {
            document.getElementById('total').value = "Ievadi ciparus :(";
            return;
        }
    }
    for(var i=0;i<4;i++) {
        totalPoints +=arr[i].value * multipliers[i];
    }
    document.getElementById('totalPoints').innerHTML=totalPoints;
}*/
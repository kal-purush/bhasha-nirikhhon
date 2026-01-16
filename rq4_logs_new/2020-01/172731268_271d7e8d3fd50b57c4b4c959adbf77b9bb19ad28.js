var PX=document.getElementById("PX");
var PY=document.getElementById("PY");

var PX1=document.getElementById("PX1");
var PY1=document.getElementById("PY1");

var PX2=document.getElementById("PX2");
var PY2=document.getElementById("PY2");

var inputPW = [PX,PY,PX1,PY1,PX2,PY2];
var count = 0 ;
$(document).ready(function(){
    $("#image").click(function(e){
        var offset = $(this).offset();
        inputPW[count].value=((e.pageX - offset.left) / $(this).outerWidth() * 100);
        inputPW[count+1].value=((e.pageY - offset.top) / $(this).outerHeight() * 100);
        count = count + 2 ;
        if(count==6){
            $('#dialog').dialog({
                titel:"conform",
                buttons:{
                    "close":()=>{
                        $('#dialog').dialog("close");
                    },
                    "Reset Patten":function(){
                        count=0;
                        PX.value="";
                        PY.value="";
                        PX1.value="";
                        PY1.value="";
                        PX2.value="";
                        PY2.value="";
                        $('#dialog').dialog("close");
                    }
                }
            });
        }
    });
});








// $(document).ready(function(){
//     $("#image").click(function(e){
//  var offset = $(this).offset();
//  PX.value=((e.pageX - offset.left) / $(this).outerWidth() * 100);
//  PY.value=((e.pageY - offset.top) / $(this).outerHeight() * 100);
// });
// });
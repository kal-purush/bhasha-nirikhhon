var jsonpatch=document.getElementById('jsonpatch').value;
var baseinput=document.getElementById('baseobj').value;
var patchtest=document.getElementById('patchex');
var newJsonObj={};
patchtest.addEventListener('click',e=>{
    formatCheck();
    
});

function formatCheck(){
    alert('inside format check');
    alert('jsonpatch'+jsonpatch);
    alert('baseinput'+baseinput);
if(jsonpatch && baseinput){
    alert('inside if');

        jsonpatchObj =isJson(jsonpatch); // Checking if the JSON patch is a valid JSON or not.
        alert('jsonpatchObj'+jsonpatchObj);
        baseinputObj = isJson(baseinput); // Checking if the base obect is a valid JSON or not.
        alert('baseinputObj'+baseinputObj);
        if(jsonpatchObj && baseinputObj){          
          newJsonObj = baseinputObj;
          alert('Hurray!! Input Base Object and/or JSON Patch is a valid JSON');
        }      
        else{
          alert('Input Base Object and/or JSON Patch is not a valid JSON');
        }
    }

}
function isJson(string){
    try{
    return JSON.parse(baseinput);}
    catch(e){
return false;
    }
}
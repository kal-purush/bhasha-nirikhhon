/// <reference path="jquery.d.ts" />
/// <reference path="cust_library.ts"/>
var addList = function(first: string, last: string){
    var newNode = document.createElement("li"); //create and li node
    newNode.innerText = first + " " + last;
    document.getElementById("thing").appendChild(newNode); //append li to ul with id="myList"
};
var validate = function() : void {
    var firstname = document.forms["form1"].elements['firstname'].value;
    var lastname = document.forms["form1"].elements['lastname'].value;
    console.log(firstname + " " + lastname);
    addList(firstname, lastname);
};
var foo = null;
var pause = 0;
function doMove() : void {
    if(pause == 50) return;
    pause ++;
    foo.style.left = parseInt(foo.style.left) +  1 + 'px'; //pseudo-property code: Move right by 10px
    setTimeout(doMove, 1); //call doMove in 20sec
}
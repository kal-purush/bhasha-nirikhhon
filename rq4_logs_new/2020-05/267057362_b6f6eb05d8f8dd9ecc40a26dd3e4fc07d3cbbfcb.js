//Examine the DOM objects//

// console.dir(document);
// console.log(document.domain);
// console.log(document.URL);
// console.log(document.title);
// //document.title =  123;
// console.log(document.doctype);
// console.log(document.head);
// console.log(document.body);
// console.log(document.all);
// console.log(document.all[10]);
// // document.all[10].textContent = 'Hello';
// console.log(document.forms[0]);
// console.log(document.links);
// console.log(document.images);




// SELECTORS


// GETELEMENTBYID //
// console.log(document.getElementById('header-title'));
// var headerTitle = document.getElementById('header-title');
// var header = document.getElementById('main-header');
// console.log(headerTitle);
// headerTitle.textContent = 'Hello';
// headerTitle.innerText = 'Goodbye';
// console.log(headerTitle.innerText);
// headerTitle.innerHTML = '<h3>Hello</h3>';
// header.style.borderBottom = 'solid 3px #000';

// GETELEMENTSBYCLASSNAME //
// var items = document.getElementsByClassName('list-group-item');
// console.log(items);
// console.log(items[1]);
// items[1].textContent = 'Hello 2';
// items[1].style.fontWeight = 'bold';
// items[1].style.backgroundColor = 'yellow';

// // Gives error
// //items.style.backgroundColor = '#f4f4f4';

// for(var i = 0; i < items.length; i++){
//   items[i].style.backgroundColor = '#f4f4f4';
// }

// GETELEMENTSBYTAGNAME //
// var li = document.getElementsByTagName('li');
// console.log(li);
// console.log(li[1]);
// li[1].textContent = 'Hello 2';
// li[1].style.fontWeight = 'bold';
// li[1].style.backgroundColor = 'yellow';

// // Gives error
// //items.style.backgroundColor = '#f4f4f4';

// for(var i = 0; i < li.length; i++){
//   li[i].style.backgroundColor = '#f4f4f4';
// }

// QUERYSELECTOR // ..selects first item only
// var header = document.querySelector('#main-header');
// header.style.borderBottom = 'solid 4px #ccc';

// var input = document.querySelector('input');
// input.value = 'Hello World'

// var submit = document.querySelector('input[type="submit"]');
// submit.value="SEND"

// var item = document.querySelector('.list-group-item');
// item.style.color = 'red';

// var lastItem = document.querySelector('.list-group-item:last-child');
// lastItem.style.color = 'blue';

// var secondItem = document.querySelector('.list-group-item:nth-child(2)');
// secondItem.style.color = 'coral';

// QUERYSELECTORALL //
// var titles = document.querySelectorAll('.title');

// console.log(titles);
// titles[0].textContent = 'Hello';

// var odd = document.querySelectorAll('li:nth-child(odd)');
// var even= document.querySelectorAll('li:nth-child(even)');

// for(var i = 0; i < odd.length; i++){
//   odd[i].style.backgroundColor = '#f4f4f4';
//   even[i].style.backgroundColor = '#ccc';
// }



// TRAVERSING THE DOM 
// var itemlist =  document.querySelector("#items");

//parentNode property
// console.log(itemlist.parentNode);
// itemlist.parentNode.style.backgroundColor = 'red';
// console.log(itemlist.parentNode.parentNode.parentNode);

//parentElement property
// (basically the same thing as parentNode )
// console.log(itemlist.parentNode);
// itemlist.parentNode.style.backgroundColor = 'red';

// childNodes vs children 
// console.log(itemlist.childNodes);
// (displays line breaks or empty spaces also as 'text' )
// not recommended 
// instead use 
    //  children
// console.log(itemlist.children);
// itemlist.children[1].style.backgroundColor = 'red';

// firstChild vs firstElementChild
// first child displays line break as well 
//  console.log(itemlist.firstChild);
//  console.log(itemlist.firstElementChild);
//  itemlist.firstElementChild.textContent = "abc";
//  itemlist.firstChild.textContent = "abc";

// lastChild vs lastElementChild
// similar to above
// itemlist.lastElementChild.style.color = "red";

// nextSibling vs nextElementSibling
// previousSibling vs previousElementSibling
//  similar to above
// itemlist.previousElementSibling.innerText = "abc "



//CREATE ELEMENT

// // Create Div 
// var newDiv = document.createElement('div');
// console.log(newDiv);

// // Add Class 
// newDiv.className = "hello";

// // Add id 
// newDiv.id = 'hello1';

// // Add attr
// newDiv.setAttribute('title', 'hello div');
// newDiv.setAttribute('class', 'class set using attr');

// // Add Text
// var text = document.createTextNode('Hello World');

// // Add text to div
// newDiv.appendChild(text)

// // Adding the  new element inside DOM 

// var container = document.querySelector('header .container');
// var h1 = document.querySelector('header h1');
// //selecting container which is inside header and h1 which is insde header resq

// container.insertBefore(newDiv, h1);
// //inside the container add newDiv before h1



// EVENT LISTENERS

//  let button = document.getElementById('button').addEventListener('click', function(){
//      console.log(123);
//  })
 //anotehr way

//  var button = document.getElementById('button').addEventListener
//  ('click', buttonClick);

//  function buttonClick(){
    //  console.log('button Clicked');
//     document.querySelector('#button').textContent ="Clicked";
//     document.getElementById('button').style.backgroundColor = "red";
//  };

//funciton with parameter
// function buttonClick(e){
//     console.log(e); //by adding other methods ( say .target )
                    //we use specifically what is listed by log(e)
    // console.log(e.target);
    // console.log(e.target.id);
    // console.log(e.target.className);
    // console.log(e.target.classList);

    //displaying in dom
//  var output = document.getElementById('output');
//  output.innerHTML = '<h3>'+e.target.id+' </h3>'

// console.log(e.type);
// console.log(e.clientX); // these are mouse positon from
// console.log(e.clientY); // browser window

// console.log(e.offsetX); // mouse position from the actual
// console.log(e.offsetY); // element that you are inside of
// //Note Mouse positons are in pixels (tested)

// console.log(e.altKey);

// }

// var button = document.getElementById('button');
// var box = document.getElementById('box');
// var output = document.getElementById('output');
// var itemInput = document.querySelector('input[type="text"]');
// var form = document.querySelector('form');
// var select = document.querySelector('select');

// button.addEventListener('click', runevent);
// button.addEventListener('dblclick', runevent);
// button.addEventListener('mousedown', runevent);
// button.addEventListener('mouseup', runevent);
// button.addEventListener('click', runevent);

// box.addEventListener('mouseenter', runevent);
// box.addEventListener('mouseleave', runevent);
// box.addEventListener('mouseover', runevent);
// box.addEventListener('mouseout', runevent);
//difference between the two: over/out shows when hovered over content also

// box.addEventListener('mousemove',runevent)


// itemInput.addEventListener('keydown', runevent);
// itemInput.addEventListener('keyup', runevent);
// itemInput.addEventListener('keypress', runevent);
// itemInput.addEventListener('focus', runevent); //click in
// itemInput.addEventListener('blur', runevent); //click out

// itemInput.addEventListener('copy', runevent);
// itemInput.addEventListener('cut', runevent);
// itemInput.addEventListener('paste', runevent);

// itemInput.addEventListener('input', runevent); //do anything within input field the firesup
// select.addEventListener('change', runevent);
// select.addEventListener('input', runevent);
// form.addEventListener('submit', runevent);

// function runevent(e){
//     e.preventDefault(); // to prevent submit event from refreshing
//     console.log('EVENT TYPE: '+ e.type); 
    // document.body.style.display = 'none'
 
    // console.log(e.target.value);
    // output.innerHTML = '<h5>' +e.target.value+ '</h5>'


//     output.innerHTML = "<h3> MouseX: "+e.offsetX+" </h3> <h3> MouseY: "+e.offsetY+" </h3>"

//     box.style.backgroundColor = "rgb("+e.offsetX+", "+e.offsetY+", 0  )"

// }
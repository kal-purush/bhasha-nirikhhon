//   Api
let mainWraperPost = document.getElementById("post-block");
let overlay = document.getElementById('overlay');
let overlayContent = document.getElementById('content');
let closeOverlay = document.getElementById('close');
let closeOverlayPost = document.getElementById('close-2');

let addButton = document.getElementById ('add-post');
let postOverlay = document.getElementById ('overlay-add');
let form = document.getElementById ('form');


function ajax(url, callback) {
    let request = new XMLHttpRequest();
    request.open('GET', url);
    request.addEventListener ('load', function (){
        let data = JSON.parse(request.responseText);
        callback(data);
    })
    request.send();
}

ajax('https://jsonplaceholder.typicode.com/posts', function(data){
    printData(data);
});

function printData(data) {
    data.forEach(item => {
        createPost(item);
    });
}

function createPost (item) {
    let divWrapper = document.createElement('div');
    divWrapper.classList.add('posts');
    divWrapper.setAttribute('data-id', item.id);
    divWrapper.addEventListener('click', function(event){
        postOverlay.classList.remove('active');
        let id = event.target.getAttribute('data-id');
        openOverlay(id);
    })

    let h2Tag =document.createElement('h2');
    h2Tag.innerText = item.id;

    let h3Tag = document.createElement('h3');
    h3Tag.innerText = item.title;

    let deleteButton = document.createElement('button');
    deleteButton.innerText = "Detele Post";
    deleteButton.classList.add('delete');
    deleteButton.setAttribute('data-id', item.id);

    divWrapper.appendChild(h2Tag);
    divWrapper.appendChild(h3Tag);
    divWrapper.appendChild(deleteButton);

    deleteButton.addEventListener('click', function(event){
        event.stopPropagation(); // ar gadasces mshobels
        let id = event.target.getAttribute('data-id');
        deletePost(id);
        divWrapper.classList.add('hide')
    })
    mainWraperPost.appendChild(divWrapper);
}


function openOverlay(id) { 
    if  (id !== null) {
        overlay.classList.add('active');
        let url = `https://jsonplaceholder.typicode.com/posts/${id}`
    
        ajax(url, function(data){
            overlayFunction(data);
        })
    } else {
        overlay.classList.add('active');
        let h2Tag =document.createElement('h2');
        h2Tag.innerText = "Item.id === null !";
        }
}

function deletePost(id) {
    let url = `https://jsonplaceholder.typicode.com/posts/${id}`;
    fetch(url, {
        method: 'DELETE',
    })
    
}

function overlayFunction (item) {
    let h2Tag =document.createElement('h2');
    h2Tag.innerText = item.id;

    let description = document.createElement('p');
    description.innerText = item.body;

    let title = document.createElement('h3');
    title.innerText = item.title;

    overlayContent.innerText = '';
    overlayContent.appendChild(h2Tag);
    overlayContent.appendChild(title);
    overlayContent.appendChild(description);
}

closeOverlay.addEventListener("click", function() {
    overlay.classList.remove('active');
})

addButton.addEventListener('click', function() {
    overlay.classList.remove('active');
    postOverlay.classList.add('active');
})

closeOverlayPost.addEventListener("click", function() {   
    postOverlay.classList.remove('active');
})

form.addEventListener('submit', function(event) {
    event.preventDefault() // vachereb default action-s
    let item = {  
        title: event.target[0].value,
        body: event.target[1].value,
    }

    fetch ('https://jsonplaceholder.typicode.com/posts', {
        method: 'POST',
        body: JSON.stringify(item),
        headers: {
            'content-type': 'application/json; charset-UTF-8',
        },
    })
    .then((response) => response.json())
    .then((json) => console.log(json));
        console.log(item);
    item.id = mainWraperPost.children.length +=1;
    createPost(item);
    postOverlay.classList.remove('active');
})
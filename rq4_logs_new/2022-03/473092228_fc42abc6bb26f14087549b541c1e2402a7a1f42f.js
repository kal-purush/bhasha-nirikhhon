const openAddWindow = function(id) {
    addId = id;
    document.querySelector('.add_form').classList.remove('hide');
}

const closeAddWindow = function() {
    document.querySelector('.add_form').classList.add('hide');
    document.querySelector('.add_info').value = '';
    document.querySelector('.add_title').value = '';
}

function hide() {
    this.classList.add('hide');
};

const addTask = function() {
    title = document.querySelector('.add_title').value;
    info = document.querySelector('.add_info').value;
    cellheight += 150;
    id_counter += 1;
    var height = cellheight.toString() + "px";
    //document.getElementsByClassName('listcell')[0].style.minHeight = height;
    var cell = document.getElementById("cell"+addId);
    let inner = '<div class = "taskcard" id = "card'+id_counter.toString()+'" draggable="true"> <div class = "taskcard-title" id = "title">'+title+'</div><div class = "taskcard-info" id = "info">'+info+'</div></div>';
    cell.insertAdjacentHTML('beforeend', inner);
    document.querySelector('.add_form').classList.add('hide');
    document.querySelector('.add_info').value = '';
    document.querySelector('.add_title').value = '';
}

/*const addTask = function(id) {
    var li = document.getElementById("cell1").cloneNode();
    document.getElementById("js-list").appendChild(li);
    li = document.getElementById("cell1").cloneNode();
    li.classList.remove('js-first');
    li.classList.add('js-second');
    document.getElementById("js-list").appendChild(li);
    li = document.getElementById("cell1").cloneNode();
    li.classList.remove('js-first');
    li.classList.add('js-third');
    document.getElementById("js-list").appendChild(li);
    var card = document.getElementById("card").cloneNode();
    var info = document.getElementById("info").cloneNode();
    info.textContent = 'Description';
    var title = document.getElementById("title").cloneNode();
    title.textContent = 'Title';
    card.appendChild(title);
    card.appendChild(info);
    switch (id) {
        case "1":
            document.getElementById("cell1").appendChild(card);
        case "2":
            document.getElementById("cell2").appendChild(card);
        case "3":
            document.getElementById("cell3").appendChild(card);
    }
    
    
}*/

/*const dragDrop = function(){
    const cells = document.querySelectorAll('.listcell');

    const allowDrop = function(event){
        event.preventDefault();
    }

    const drag = function(event){
        event.dataTransfer.setData('id', event.target.id);
    }

    const drop = function(event){
        let itemId = event.dataTransfer.getData('id');
        event.target.append(document.getElementById(itemId));
    }

    cells.forEach((cell) => {
        cell.ondragover = allowDrop;
        cell.ondrop = drop;
    });
    
    const cards = document.querySelectorAll('.taskcard');

    cards.forEach((card) => {
        card.ondragstart = drag;
    })
}*/

function dragDrop(){
    const cards = document.querySelectorAll('.taskcard');
    const cells = document.querySelectorAll('.listcell');

    const dragStart = function(event) {
        setTimeout(() => {
            this.classList.add('hide');
        }, 10);
        event.dataTransfer.setData('id', event.target.id);

    };

    const dragEnd = function() {
        this.classList.remove('hide');
    };

    const dragOver = function(evt) {
        evt.preventDefault();
    };

    const dragEnter = function(evt) {
        evt.preventDefault();
        this.classList.add('hovered');
    };

    const dragLeave = function() {
        this.classList.remove('hovered');
    };

    const dragDrop = function(event) {
        if (this.classList[2] == 'hovered') {
            this.classList.remove('hovered');
            // this.insertAdjacentHTML('beforeend', '<div class = "taskcard" id = "card" draggable="true"> <div class = "taskcard-title" id = "title">Title</div><div class = "taskcard-info" id = "info">Description</div></div>');
            // this.append(card);
            let itemId = event.dataTransfer.getData('id');
            if(refId == -1){
                this.append(document.getElementById(itemId));
            } else{
                console.log(1232321);
                refId = event.dataTransfer.getData('refid');
                event.target.insertBefore(document.getElementById(itemId), document.getElementById(refId));
            }
        }
    };

    const setId = function(event) {
        //event.dataTransfer.setData('refid', event.target.id);
    };

    const removeId = function(event) {
        //event.dataTransfer.setData('refid', -1);
    };

    cells.forEach((cell) => {
        cell.addEventListener('dragover', dragOver);
        cell.addEventListener('dragenter', dragEnter);
        cell.addEventListener('dragleave', dragLeave);
        cell.addEventListener('drop', dragDrop);
    });

    cards.forEach((card) => {
        card.addEventListener('dragstart', dragStart);
        card.addEventListener('dragend', dragEnd);
        card.addEventListener('dragenter', setId);
        card.addEventListener('dragleave', removeId);
    });
    
    
};

var cellheight = 150;
var id_counter = 1;
var k = 1;
setInterval(dragDrop, 100);
let refId = -1;
let addId;
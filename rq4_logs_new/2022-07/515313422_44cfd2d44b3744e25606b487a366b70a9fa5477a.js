var form = document.getElementById('addForm');
var itemList = document.getElementById('items');

//Form submit Event
form.addEventListener('submit', additem);

itemList.addEventListener('click', removeItem);

function additem(e) {
    e.preventDefault();

   var newItem = document.getElementById('item').value;
   
   var li = document.createElement('li');
   li.className = 'list-group-item';
   
   li.appendChild(document.createTextNode(newItem));

   var deleteBtn = document.createElement('button');

   deleteBtn.className = 'btn btn-danger btn-sm float-right delete';

   deleteBtn.appendChild(document.createTextNode('X'));

   li.appendChild(deleteBtn);

   itemList.appendChild(li);

   var editBtn = document.createElement('button');
   editBtn.className = 'btn btn-sm float-right edit';

   editBtn.appendChild(document.createTextNode('Edit'));
   li.appendChild(editbtn);
   itemList.appendChild(li);
    
}



function removeItem(e) {
    if(e.target.classList.contains('delete')) {
        if(confirm('Are you sure')) {
            var li = e.target.parentElement;
            itemList.removeChild(li);
        }
    }
}
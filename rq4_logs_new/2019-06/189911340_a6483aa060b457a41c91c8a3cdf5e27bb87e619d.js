var accBtn = document.querySelector('#accBtn');
var addBtn = document.querySelector('#addBtn');
var editDelBtn = document.querySelector('#editDelBtn');
var mainView = document.querySelector('#mainView');
var mainBody = document.querySelector('#mainBody');
var formView =document.querySelector('#formView');
var saveBtn = document.querySelector('#saveBtn');
var accId = document.querySelector('#accId');
var eaccId = document.querySelector('#eaccId');
var accName = document.querySelector('#accName');
var accDeposit = document.querySelector('#accDeposit');
var accCard = document.querySelector('#accCard');
var eaccName = document.querySelector('#eaccName');
var eaccDeposit = document.querySelector('#eaccDeposit');
var eaccCard = document.querySelector('#eaccCard');
var editView =document.querySelector('#editView');
var editBody = document.querySelector('#editBody');
var editFormView = document.querySelector('#editFormView');
var editBtn = document.querySelector('#editBtn');
var id;
var db = [
  {
    id : '1',
    name : 'Edo',
    deposit : '12000€',
    cCard : 'Visa'
  },
  {
    id : '2',
    name : 'Selma',
    deposit : '11000€',
    cCard : 'Maestro'
  }
];
// *************************************************//
addBtn.addEventListener('click', showForm);
accBtn.addEventListener('click', showMainView);
saveBtn.addEventListener('click', saveAccount);
editDelBtn.addEventListener('click', showEditView);
editBtn.addEventListener('click', changeAccount);
// *************************************************//
createTable();

function createTable() {
  var text = '';
  for (var i = 0; i<db.length; i++) {
    text += '<tr>';
    text += '<td>'+db[i].id+'</td>';
    text += '<td>'+db[i].name+'</td>';
    text += '<td>'+db[i].deposit+'</td>';
    text += '<td>'+db[i].cCard+'</td>';
    text += '</tr>';
  };
  mainBody.innerHTML = text;
}

function showEditView() {
  var text = '';
  for (var i = 0; i<db.length; i++) {
    text += '<tr>';
    text += '<td>'+db[i].id+'</td>';
    text += '<td>'+db[i].name+'</td>';
    text += '<td>'+db[i].deposit+'</td>';
    text += '<td>'+db[i].cCard+'</td>';
    text += '<td><button data-id="'+i+'" class="btn btn-warning edit">Edit</button></td>';
    text += '<td><button id="'+i+'" class="btn btn-danger delete">Delete</button></td>';
    text += '</tr>';
  };
  editBody.innerHTML = text;
  var deleteBtns = document.querySelectorAll('.delete');
  var editBtns = document.querySelectorAll('.edit');
  for (var i = 0; i < deleteBtns.length; i++) {
    deleteBtns[i].addEventListener('click', deleteAccount);
    editBtns[i].addEventListener('click', editAccount);
  }

function editAccount() {
  mainView.style.display = 'none';
  formView.style.display = 'none';
  editView.style.display = 'none';
  editFormView.style.display = 'block';

  id = this.getAttribute('data-id');
  eaccId.value = db[id].id;
  eaccName.value = db[id].name;
  eaccDeposit.value = db[id].deposit;
  eaccCard.value = db[id].cCard;

}
function deleteAccount() {
  var id = this.id;
  db.splice(id,1);
  createTable();
  showMainView();
}

  mainView.style.display = 'none';
  formView.style.display = 'none';
  editFormView.style.display = 'none';
  editView.style.display = 'block'
}

function showForm() {
  formView.style.display = 'block';
  mainView.style.display = 'none';
  editFormView.style.display = 'none';
  editView.style.display = 'none';
}
function changeAccount() {
  var accId = eaccId.value;
  var accName = eaccName.value;
  var accDeposit = eaccDeposit.value;
  var accCard = eaccCard.value;
  db [id] = {
    id : accId,
    name : accName,
    deposit : accDeposit,
    cCard : accCard

  };
  createTable();
  showMainView();
}
function showMainView() {
  formView.style.display = 'none';
  mainView.style.display = 'block';
  editFormView.style.display = 'none';
  editView.style.display = 'none';
}

function saveAccount() {
  var id = accId.value;
  var name = accName.value;
  var deposit = accDeposit.value;
  var card = accCard.value;
  var newAccount = {
    id : id,
    name : name,
    deposit : deposit,
    cCard : card
  };
  db.push(newAccount);
  createTable();
  showMainView();
}
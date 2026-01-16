// Sayfa yapısı

const registerTab = document.getElementById('register')
const candidatesTab = document.getElementById('candidates')
const changeTabBtn = document.querySelector('#changeTab')

let showList = 1

const changeTab = function () {
  showList = !showList
  if (showList == 0) {
    registerTab.style.display = 'none'
    candidatesTab.style.display = 'block'
  } else {
    registerTab.style.display = 'block'
    candidatesTab.style.display = 'none'
  }
}

// Yeni Kayıt işlemleri

const saveBtn = document.getElementById('saveBtn')
const clearBtn = document.getElementById('clearBtn')
const addForm = document.getElementById('addForm')

const emailTxt = document.getElementById('applyEmail')
const fullnameTxt = document.getElementById('applyFullname')
const phoneTxt = document.getElementById('applyPhone')
const birthdayTxt = document.getElementById('applyBirthday')

let campApps =
  localStorage.getItem('campapps') == null
    ? []
    : JSON.parse(localStorage.getItem('campapps'))

clearBtn.addEventListener('click', () => {
  addForm.reset()
})

saveBtn.addEventListener('click', (event) => {
  event.preventDefault()
  if (
    emailTxt.value == '' ||
    fullnameTxt.value == '' ||
    phoneTxt.value == '' ||
    birthdayTxt.value == ''
  ) {
    alert('Lütfen bilgilerinizi giriniz.')
    for (let index = 0; index < campApps; index++) {
      const element = campApps[index]
      console.log(element)
    }
  } else {
    campApps.push(new NewApply(emailTxt.value, fullnameTxt.value, phoneTxt.value, birthdayTxt.value))
    localStorage.setItem('campapps', JSON.stringify(campApps))
    addForm.reset()
    renderList()
  }
})

class NewApply {
  constructor(email, fullname, phonenumber, birthday) {
    this.email = email
    this.fullname = fullname
    this.phonenumber = phonenumber
    this.birthday = moment(birthday, 'YYYY-MM-DD').format('DD/MM/YYYY')
    this.createdDate = moment().format("DD/MM/YYYY HH:MM")
  }
}

// Liste işlemleri
const tbodyList = document.getElementById("candidatesList").getElementsByTagName("tbody")[0]

const renderList = function(){
    tbodyList.innerHTML = ""
    for (let index = campApps.length - 1; index => 0; index--) {
        let element = campApps[index]
        let rowTemplate = `
        <tr>
            <td>${index + 1}</td>
            <td>${element.fullname}</td>
            <td>${element.email}</td>
            <td>${element.phonenumber}</td>
            <td>${element.birthday}</td>
            <td>${element.createdDate}</td>
            <td><button class="btn btn-danger btn-sm">Reddet</button></td>
            <td><button class="btn btn-success btn-sm">Kabul Et</button></td>
        </tr>
        `
        tbodyList.innerHTML += rowTemplate;
    }
}
renderList();
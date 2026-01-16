const god = document.querySelector('.Good');
setInterval(() => {
  let god = document.querySelector('.Good');
  let hr = document.querySelector('.hr');
  let mn = document.querySelector('.mn');
  let sc = document.querySelector('.sc');

  const date = new Date();
  hr = date.getUTCHours() - 3;
  mn = date.getUTCMinutes();
  sc = date.getUTCSeconds();

  /* hr = hr < 10 ? '0'+hr : hr;
  mn = mn < 10 ? '0'+mn : mn;
  sc = sc < 10 ? '0' + sc : sc; */

  if( hr => '00' && hr <= '05'){
    god.innerText = 'Boa madrugada!'
  }
  else if(hr => '06' && hr <= '11'){
    god.innerText = 'Bom dia!'
  }
  else if(hr => '12' && hr <= '17'){
    god.innerText = 'Boa tarde!'
  }
  else if(hr => '18' && hr <= '23'){
    god.innerText = 'Boa noite!'
  }
  /* god.innerText = `${hr}:${mn}:${sc}`; */
}, 1)

const teste = document.querySelector('.teste');
const testeDois = document.querySelector('.teste-dois');

/* 





teste.addEventListener('click', () => getUserName());

testeDois.addEventListener('click', () => deleteName());

checkUser(); */

const nameUser = document.querySelector(".nameUser");
const tttt = document.querySelector('.body-input-name');
const userName = document.querySelector('.get-name-user')

nameUser.addEventListener("click", () => {
  tttt.style.display = 'flex'

  nameUser.style.display = 'none'
})

const fechar = document.querySelector(".fechar");
fechar.addEventListener('click', () => {

  tttt.style.display = 'none'
  nameUser.style.display = 'block'
})

function checkUser() {
  const user = localStorage.getItem('name');

  if(user){
    areaInputName.style.display = 'none'
  }else{
    const areaInputName = document.querySelector('.areaOfInputName');
    areaInputName.style.display = 'flex'
  }
}

const getUserName = () => {
  const userName = document.querySelector('.input-name').value

  localStorage.setItem('name', userName);

  checkUser()
}

userName.userName('click', () => getUserName())
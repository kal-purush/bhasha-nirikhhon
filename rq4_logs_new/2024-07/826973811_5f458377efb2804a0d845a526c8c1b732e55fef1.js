const welcomeMsg=document.getElementById('welcomemsg');
let logOutBtn=document.getElementById('btn');

if(localStorage.getItem('userName') != null){
    welcomeMsg.innerHTML = `Welcome ${localStorage.getItem('userName')}!`
}

logOutBtn.addEventListener('click',function(){
    localStorage.removeItem('userName');
    location.href ="login.html"
})
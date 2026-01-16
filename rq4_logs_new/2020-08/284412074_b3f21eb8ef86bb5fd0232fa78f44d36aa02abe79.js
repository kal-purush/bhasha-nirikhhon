let myForm = document.querySelector('#my-Form');
let myEmail = document.querySelector('#Email');
let myPassword = document.querySelector('#passWord');
let msgError1 = document.querySelector('#span1');
let msgError2 = document.querySelector('#span2');
let msgSuccess = document.querySelector('#span3');
let errorBorder1 = document.querySelector('input[type="email"]');
let errorBorder2 = document.querySelector('input[type="password"]');

msgError1.innerHTML = '<small>(eg. example@gmail.com)</small>';
msgError1.style.color = 'grey';

msgError2.innerHTML = '<small>(eg. It can be mixture of 0-9 , A-z , a-z , @ , $ are allowed)</small>';
msgError2.style.color = 'grey';

myForm.addEventListener('submit',onSubmit);
myForm.addEventListener('reset',onreset);

function onSubmit(){
    event.preventDefault();
    if(myEmail.value === '' && myPassword.value === ''){
        msgError1.innerHTML ='<small>Please fill the input field!</small>';
        msgError1.style.color = 'red';

        msgError2.innerHTML ='<small>Please fill the input field!</small>';
        msgError2.style.color = 'red';

        errorBorder1.style.borderColor ='red';
        errorBorder1.style.focus = 'red';
        errorBorder2.style.borderColor ='red';
        errorBorder2.style.focus = 'red';
    }
    else if(!(myEmail.value === '') && (myPassword.value === '')){
        msgError1.innerHTML = '<small>(eg. example@gmail.com)</small>';
        msgError1.style.color = 'grey';
        errorBorder1.style.borderColor ='green';
        msgError2.innerHTML ='<small>Please fill the input field!</small>';
        msgError2.style.color = 'red';
        errorBorder2.style.borderColor ='red';
    }
    
    else if((myEmail.value === '') && !(myPassword.value === '')){
        msgError1.innerHTML ='<small>Please fill the input field!</small>';
        msgError1.style.color = 'red';
        errorBorder1.style.borderColor ='red';
        msgError2.innerHTML = '<small>(eg. It can be mixture of 0-9 , A-z , a-z , @ , $ are allowed)</small>';
        msgError2.style.color = 'grey';
        errorBorder2.style.borderColor ='green';
    }
    else {
        msgError1.innerHTML = '<small>(eg. example@gmail.com)</small>';
        msgError1.style.color = 'grey';
        msgError2.innerHTML = '<small>(eg. It can be mixture of 0-9 , A-z , a-z , @ , $ are allowed)</small>';
        msgError2.style.color = 'grey';
        msgSuccess.innerHTML ='<center>SignUp Successfull!!</center>';
        msgSuccess.style.color = 'green';
        errorBorder1.style.borderColor ='#ced4da';
        errorBorder2.style.borderColor ='#ced4da';
    }
    
   
}

function onreset(){
    myEmail.value = '';
    myPassword.value ='';
    msgSuccess.innerHTML ='';
    
}
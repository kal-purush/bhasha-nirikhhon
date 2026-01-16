let forgotPass = document.getElementById('forgotPassword');
let content = document.querySelector('.content-box');
let login_btn = document.getElementById('login-btn');
let username = document.getElementById('username');
let password = document.getElementById('password');

let modal = document.querySelector('.modal');
let close = document.getElementById('close-modal');
let modal_timer = document.getElementById('modal-timer');
let resend = document.getElementById('resend-btn');
let enter_email = document.getElementById('enter-email-btn');
let enter_email_box = document.querySelector('.enter-email-box');
let email = document.getElementById('email');
let errorMessage = document.querySelector('.errorMessage');
let resendBox = document.querySelector('.modal-box-bottom');

/*
KASIH COMMENT BUAT SETIAP FUNCTION
*/

function validation(){
    //alert(username.value);
    if(username.value.trim()==""){
        errorMessage.style.display="flex";
        errorMessage.innerText="Username required";
        return false;
    }
    else if(password.value.trim()==""){
        errorMessage.style.display="flex";
        errorMessage.innerText="Password required";
        return false;
    }
    else{
        errorMessage.style.display="none";
        return true;
    }
}

// let body = document.querySelector('body');
forgotPass.addEventListener('click',function(e){
    e.preventDefault();
    // alert("MASUKKK");
    modal.style.opacity=1;
    modal.style.pointerEvents="auto";

    resend.style.textDecoration="none";
    resend.style.color="black";
    modal_timer.innerText='';
});

login_btn.addEventListener('click',function(e){
    if(!validation()){
        e.preventDefault();
    }
    // username.value=null;
    // password.value=null;
});



let errorMessage_modal = document.getElementById('modal_errorMessage');

function resets(){
    errorMessage_modal.style.display="none";
    errorMessage_modal.style.pointerEvents="none";
    resendBox.style.display="none";
    resendBox.style.pointerEvents="none";
}

function validateEmail(email) {
    const re = /\S+@\S+\.\S+/;
    return re.test(email);
}


function validation_modal(){
    //&& !(email.value.includes('@'))
    // if(email.value.trim()==""){
    if(!validateEmail(email.value.trim())){
        // alert("ESTER");
        errorMessage_modal.style.display="flex";
        errorMessage_modal.innerText="Email is Required";
        return false;
    }
    else{
        errorMessage_modal.innerText="";
        errorMessage_modal.style.display="none";
        return true;
    }
}

close.addEventListener('click',function(e){
    modal.style.opacity=0;
    clearInterval(intervalTimer);
    enter_email_box.reset();
    modal.style.pointerEvents="none";
    errorMessage_modal.style.display="none";
    errorMessage_modal.style.pointerEvents="none";
    resendBox.style.display="none";
    resendBox.style.pointerEvents="none";
});

let intervalTimer;
function Timer(){
    let second=30;
    modal_timer.innerText='in '+second.toString()+'s';

    intervalTimer = setInterval(function(e){
        second=second-1;
        modal_timer.innerText='in '+second.toString()+'s';
        // alert("NGITUNG");
        if(second==0){
            // alert("MASUK");
            resend.style.textDecoration="underline";
            resend.style.color="#6C9493";
            resend.style.cursor="pointer";
            modal_timer.innerHTML="";
            clearInterval(intervalTimer);
        }
    },1000);
}


resend.addEventListener('click',function(e){
    //alert("Event Lister BERHASIL");
    if(validation_modal()){
        if(resend.style.textDecoration=="underline"){
            errorMessage_modal.style.display="none";
            resend.style.textDecoration="none";
            resend.style.color="black";
            resend.style.cursor="default";
            modal_timer.innerText='in 29s';
            //alert("MASUK FUNCTION");
            Timer();
            // resendBox.style.display="flex";
        }
    }
});


enter_email.addEventListener('click',function(e){
    e.preventDefault();
    resets();
    clearInterval(intervalTimer);
    if(validation_modal()){
        Timer();
        resendBox.style.display="flex";
        resendBox.style.pointerEvents="auto";
    }
});


/*
INI COBA VALIDAASIIN DLU KLO INI MMG EMAIL, KALAU BKN EMAIL JANGAN DIPROSES
COBA PASTIIN SM BACKEND, BOLEH GAK BUTTON-NYA PAKE TYPE BUTTON AJA
TERAKHIR, JANGAN SAMPE TIMER di EMAIL TABRAKAN SAMA TIMER DI BUTTON ARROW */
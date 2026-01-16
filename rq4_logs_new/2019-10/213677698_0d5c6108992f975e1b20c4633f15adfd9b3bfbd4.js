((submitData)=>{
    function userData(){
        let userName = document.querySelector('#name').value;
        let userSurname = document.querySelector('#surname').value;
        let userAge = parseFloat(document.querySelector('#age').value);
        let userEmail = document.querySelector('#email').value;
        let userPassword = document.querySelector('#password').value;
        let userCPassword = document.querySelector('#confrirm-password').value;

        let inputData = [];

        if ( userPassword === userCPassword ) {
            inputData.push(userName,userSurname,userAge,userEmail,userPassword,userCPassword);
            console.log(inputData);
            alert ('success');
        } else {
            alert ('passwords do not match');
        }
    }

    let submit = document.getElementById('submit');
    submit.addEventListener('click',userData);
})();
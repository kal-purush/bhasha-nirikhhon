const form = document.getElementById('form');
const nameUF = document.getElementById('name_uf');
const typeUF = document.getElementById('type_uf');
const heightUF = document.getElementById('height_uf');
const valueUF = document.getElementById('value_uf');
const submitUF = document.getElementById('submit_uf');

form.addEventListener("submit", (event) => {
    event.preventDefault();
    
    checkInputName();
    checkInputType();
    checkInputHeight();
    checkInputValue();
    validateButton();
})

function checkInputName() {
    const nameUFvalue = nameUF.value;

    if (nameUFvalue === "") {
        errorInput(nameUF, "Preencha o nome da UF!")
    }
    else {
        const formItem = nameUF;
        formItem.className = "input"
    }
}

function checkInputType() {
    const typeUFvalue = typeUF.value;

    if (typeUFvalue === "") {
        errorInput(typeUF, "Preencha o tipo da UF!")
    }
    else {
        const formItem = typeUF;
        formItem.className = "input"
    }
}

function checkInputHeight() {
    const heightUFvalue = heightUF.value;

    if (heightUFvalue === "") {
        errorInput(heightUF, "Preencha a Altura da UF!")
    }
    else {
        const formItem = heightUF;
        formItem.className = "input"
    }
}


function checkInputValue() {
    const valueUFvalue = valueUF.value;

    if (valueUFvalue === "") {
        errorInput(valueUF, "Preencha a Potência ou Corrente da UF!")
    }
    else {
        const formItem = valueUF;
        formItem.className = "input"
    }
}

function validateButton() {
    if (nameUF.value != "" && typeUF.value != "" && heightUF.value != "" && valueUF.value != "") {
        submitUF.textContent = "Cadastrado com Sucesso";
    } 
    else {
        submitUF.textContent = "Preencha todos os campos!";
    }
}

function errorInput(input, message) {
    const formItem = input.parentElement;
    const inputs = formItem.querySelectorAll("input");

    inputs.forEach((input) => {
        input.className = "input error";
    });
    
    input.placeholder = message;
}

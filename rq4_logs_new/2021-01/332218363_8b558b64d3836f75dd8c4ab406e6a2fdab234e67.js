//Coletando ID'S do 1° INPUT;
const first_input_container = document.getElementById('first-input');
const name_input = document.getElementById('name-input');

//Coletando ID'S do 2° INPUT;
const second_input_container = document.getElementById('second-input');
const last_name_input = document.getElementById('last-name-input');

//Coletando ID'S do 3° INPUT;
const third_input_container = document.getElementById('third-input');
const email_input = document.getElementById('email-input');

//Coletando ID'S do 4° INPUT;
const fourth_input_container = document.getElementById('fourth-input');
const password_input = document.getElementById('password-input');

//Coletando ID do Botão principal.
const btn_submit = document.getElementById('btn-submit');

//Função que dispara ao pressionar o Botão principal.
const onSubmit = () => {
    btn_submit.addEventListener('click', () => {
        if(name_input.value === ""){
            let paragraph = document.createElement('p');
            paragraph.style.color = 'hsl(0, 100%, 74%)';
            paragraph.innerText = 'Você precisa preencher este campo.';
            paragraph.style.marginTop = '10px';
            paragraph.style.fontSize = '10px';
            paragraph.style.textAlign = 'end';
            paragraph.style.fontWeight = 'bold';
            first_input_container.appendChild(paragraph);
            name_input.style.border = '2px solid hsl(0, 100%, 74%)';
        }

        if(last_name_input.value === ""){
            let paragraph = document.createElement('p');
            paragraph.style.color = 'hsl(0, 100%, 74%)';
            paragraph.innerText = 'Você precisa preencher este campo.';
            paragraph.style.marginTop = '10px';
            paragraph.style.fontSize = '10px';
            paragraph.style.textAlign = 'end';
            paragraph.style.fontWeight = 'bold';
            second_input_container.appendChild(paragraph);
            last_name_input.style.border = '2px solid hsl(0, 100%, 74%)';
        }

        if(email_input.value === ""){
            let paragraph = document.createElement('p');
            paragraph.style.color = 'hsl(0, 100%, 74%)';
            paragraph.innerText = 'Você precisa preencher este campo.';
            paragraph.style.marginTop = '10px';
            paragraph.style.fontSize = '10px';
            paragraph.style.textAlign = 'end';
            paragraph.style.fontWeight = 'bold';
            third_input_container.appendChild(paragraph);
            email_input.style.border = '2px solid hsl(0, 100%, 74%)';
        }

        if(password_input.value === ""){
            let paragraph = document.createElement('p');
            paragraph.style.color = 'hsl(0, 100%, 74%)';
            paragraph.innerText = 'Você precisa preencher este campo.';
            paragraph.style.marginTop = '10px';
            paragraph.style.fontSize = '10px';
            paragraph.style.textAlign = 'end';
            paragraph.style.fontWeight = 'bold';
            fourth_input_container.appendChild(paragraph);
            password_input.style.border = '2px solid hsl(0, 100%, 74%)';
        }
    })
}

onSubmit()
const Adicionar = document.querySelector('#adicionar-paciente');
const validac = document.querySelector('#peso');
const validad = document.querySelector('#altura');
Adicionar.addEventListener('click', (event) => {
    event.preventDefault();
    const form =  document.querySelector('#form-adicionar');
    const pacient = obterdados(form);

    if(!validandopeso(pacient)){
        return
    }
    if(!validandoaltura(pacient)){
        return
    }
    add(pacient)
   
    form.reset()
})

function add(pacient) {
    let pacientetr = criartr(pacient)
    let tabela  = document.querySelector("#tabela-pacientes");
    tabela.appendChild(pacientetr);
}

function obterdados(form){
    let pacientess = {
        nome: form.nome.value,
        pesos: form.peso.value,
        alturas: form.altura.value,
        gordura: form.gordura.value,
        imc: imc(form.peso.value,form.altura.value)
    }
    return pacientess;
}
function criartr(pacient) {
    let pacientetr = document.createElement("tr");
    pacientetr.classList.add('paciente');
    
    let nometd  = criartd(pacient.nome, "info-nome");
    let pesotd  = criartd(pacient.pesos,"info-peso");
    let alturatd  = criartd(pacient.alturas, "info-altura");
    let gorduratd  = criartd(pacient.gordura, "info-gordura");
    let imctd = criartd(imc(pacient.pesos,pacient.alturas), "info-imc");

    pacientetr.appendChild(nometd);
    pacientetr.appendChild(pesotd);
    pacientetr.appendChild(alturatd);
    pacientetr.appendChild(gorduratd);
    pacientetr.appendChild(imctd);

    return pacientetr
}

function criartd(dado,classe) {
    var td = document.createElement("td");
    td.textContent = dado;
    td.classList.add(classe);
    return td;
}
function validandopeso(paciente) {
    
    if (parseInt(paciente.pesos,10) > 0 && parseInt(paciente.pesos,10) < 1000) {
        validac.classList.remove('borda');
        return true;
    }
    validac.classList.add('borda');
    return false
}
function validandoaltura(paciente) {
    console.log('entrou')
    if (parseInt(paciente.alturas,10) > 0 && parseInt(paciente.alturas,10) < 3) {
        validad.classList.remove('borda');
        return true;
    }
    validad.classList.add('borda');
    return false;
}
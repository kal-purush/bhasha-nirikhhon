const form = document.querySelector('.form');

form.addEventListener('submit', 
function (event){
    event.preventDefault();
    const inputPeso = event.target.querySelector("#input-peso")
    const inputAltura = event.target.querySelector("#input-altura")

    const peso = Number(inputPeso.value);
    const altura = Number(inputAltura.value)
    if (!peso){
        setResultado("Peso invalido", false)
        return;
    }
    if (!altura){
        setResultado("Altura invalido", false)
        return;
    }
    
    const imc = getImc(peso,altura);
    const nivelImc = getIndImc(imc);

    const msg = `Seu IMC é ${imc} (${nivelImc}).`;
    setResultado(msg, true);
});
function criaP(){
    const p = document.createElement('p');
    return p;
}
function setResultado(msg, isValid){
    const resultado = document.querySelector("#resultado");
    resultado.innerHTML = '';
    const p = criaP();
    p.innerHTML = msg;
    if (isValid){
        p.classList.add('paragrafo-resultado');  
    } else{
        p.classList.add("bad")
    }
    
    resultado.appendChild(p);
}
function getImc(peso, altura){
    console.log(peso,altura)
    const imc = peso/(altura**2)
    return imc
}
function getIndImc(imc){
    const nivel = ["Abaixo do peso", "Peso normal", "Sobrepeso", "Obesidade grau 1", "Obesidade grau 2", "Obesidade grau 3"];
    if(imc >= 39.9) return nivel[5]; 
   
    if(imc >= 34.9) return nivel[4];

    if(imc >= 29.9) return nivel[3];
    
    if( imc >= 24.9) return nivel[2];
    
    if( imc >= 18.9) return nivel[1];
    
    if(imc < 18.5) return nivel[0];
}
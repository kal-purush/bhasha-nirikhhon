const ul = document.createElement('ul');

const div = document.querySelector('.conteiner');
const paragrafo = document.createElement('p');
const paragrafo2 = document.createElement('p');


//Essa função serve para criar elementos na lista

function inicia(){
   
    div.appendChild(ul);
    div.appendChild(paragrafo);
    div.appendChild(paragrafo2);
}
window.addEventListener('load', inicia())

//Criando a classe funcionarios
class funcionarios{
    constructor(){
        this.id=1;
        this.arrayFuncionarios=[];

    }
    adicionar(){
        let funcionario = this.dadosFuncionarios();
        if(this.validaCampos(funcionario)){
            this.adicionarArray(funcionario);
        }
        console.log(funcionario);
        console.log(this.arrayFuncionarios);

        return [funcionario.nome, funcionario.salario];
    }

    adicionarArray(funcionario){
        this.arrayFuncionarios.push(funcionario);
        this.id++;
    }
    

    validaCampos(funcionario){
        let mensagem='';

        if(funcionario.nome ==''){
            mensagem+='Insira o nome do funcionario.'
        }
        if(funcionario.salario==''){
            mensagem+='Insira o salario do funcionario.'
        }
        if(mensagem!=''){
            alert(mensagem)
            return false
        }

        return true;
    }
    dadosFuncionarios(){
        let funcionario={}
        funcionario.nome = document.querySelector('#nome').value;
        funcionario.salario = parseInt(document.querySelector('#salario').value);

        return funcionario;
    }
    maiorSalario(){
        let maior = this.arrayFuncionarios
        .filter(pegaSalario => pegaSalario.salario)
        .map(pegaSalario => pegaSalario.salario)
        let maioSalario = Math.max.apply(null, maior);
        return maioSalario;
 
     }
     somaSalarios(){
        
        let soma = this.arrayFuncionarios.reduce((valPrev, elemento) => valPrev + elemento.salario, 0);
        return soma;
     }
}

let funcionario = new funcionarios();


const btn = document.querySelector('.botao');

btn.addEventListener('click', ()=>{
    const li = document.createElement('li');
    ul.appendChild(li);
    
    li.textContent='Funcionario cadastrado: ' + funcionario.adicionar();
     paragrafo.textContent='O maior salario é: ' + funcionario.maiorSalario();
     paragrafo2.textContent='A soma dos salarios: '+ funcionario.somaSalarios();
   
})
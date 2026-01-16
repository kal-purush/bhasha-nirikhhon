// Hospital
// cria classe
// herança
class Hospital {
    #caso
    constructor(nome, leito , profissao , caso) {
        this.nome = nome;
        this.leito = leito;
        this.profissao = profissao;
        this.#caso = caso;
    };

    apresentar () {
        return `Olá, sou ${this.nome} e atuo no leito ${this.leito}.`;
    } 
}

class funcionario extends Hospital{
    apresentar () {
        return `sou ${this.nome} e estou trabalhando no leito ${this.leito} como ${this.profissao}`;
    }
}
   class paciente extends Hospital{
    apresentar () {
        return `sou ${this.nome} e estou em espera`;
    }
}

class chefe extends Hospital{
    apresentar () {
        return `sou ${this.nome} e estou comandando o Hospital`
    } 
}

// objeto

const roberto = new funcionario ("dr. Roberto", 12 , "Medico" )
const eduarda = new paciente ("Eduarda", "covid")
const marcos = new chefe ("Marcos")
const luiza = new funcionario ("Luiza", 10 , "enfermeira")
const thiago = new funcionario ("Thiago", 5 , "auxiliar de enfermagem")
const guilherme = new paciente ("Guilherme" , "pneumonia")
const kailany = new chefe ("Kailany")
const thalita = new funcionario ("dr. Thalita", 9 , "Medica")


eduarda.apresentar();
roberto.apresentar();
marcos.apresentar();
luiza.apresentar();
thiago.apresentar();
guilherme.apresentar();
kailany.apresentar();
thalita.apresentar();


function exibir(mensagem) {
    const output = document.getElementById("output");
    const p = document.createElement("p");
    p.textContent = mensagem;
    output.appendChild(p);
}


exibir(eduarda.apresentar());

exibir(roberto.apresentar());
exibir(marcos.apresentar());

exibir(luiza.apresentar());
exibir(thiago.apresentar());
exibir(guilherme.apresentar());
exibir(kailany.apresentar());
exibir(thalita.apresentar());


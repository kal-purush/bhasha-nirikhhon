let proto_pessoa = {
    fale: function() {
        return "oi";
    }
};

function pessoa(nome_novo, idade_nova) {

    return {
        nome: nome_novo,
        idade: idade_nova,
        fala: proto_pessoa.fale
    };
}

function pessoa1(nome_novo, idade_nova) {
    let pessoa = {};
    pessoa.__proto__ = proto_pessoa;
    pessoa.nome = nome_novo;
    pessoa.idade = idade_nova;
    return pessoa;
}

function pessoa2(nome_novo, idade_nova) {
    let nome = nome_novo;
    let idade = idade_nova;
    let pessoaSaida = function criaPessoa() {
        return {


            setNome: function(_nome) {
                nome = _nome;
            },
            setIdade: function(_idade) {
                idade = _idade;
            },
            getNome: () => nome,
            getIdade: () => idade
        };
    };
    pessoaSaida.__proto__ = proto_pessoa;
    return pessoaSaida;
}

function Pessoa(nome_novo, idade_nova) {
    this.nome = nome_novo;
    this.idade = idade_nova;
    this.__proto__ = proto_pessoa;
}

class PessoaClass {

    
    nome;
    idade;
    constructor(_nome, idade) {
        this.#nome = _nome;
        this.#idade = idade;
        this.__proto__ = proto_pessoa;
    }

    getNome() {
        return this.nome;
    }

    setNome(_nome) {
        this.nome = _nome;
    }

    getIdade() {
        return this.idade;
    }
    setIdade(_idade) {
        this.idade = _idade;
    }
}


p = pessoa("caio", 18);
p1 = pessoa1("caio", 18);
p2 = new Pessoa("caio", 18);
p3 = pessoa2("caio", 18);
p4 = new PessoaClass("max", 30);

console.log(p1.fale());
console.log(p.__proto__);
console.log(p2.fale());
console.log(p3.__proto__);
console.log(p4.nome);
//console.log(p4.fale());


class heroi{

    constructor(nome, idade, tipo){
        this.nome = nome
        this.idade = idade
        this.tipo = tipo
    }

    atacar(){
    let ataque = null

       if (this.tipo === "mago"){
            let ataque = "magia"
            console.log(`O ${this.tipo} atacou usando ${ataque}`)
       }else if (this.tipo === "guerreiro"){
            let ataque = "espada"
            console.log(`O ${this.tipo} atacou usando ${ataque}`)
       }else if (this.tipo === "monge"){
            let ataque = "artes maciais"
            console.log(`O ${this.tipo} atacou usando ${ataque}`)
       }else if (this.tipo === "ninja"){
            let ataque = "shuriken"
            console.log(`O ${this.tipo} atacou usando ${ataque}`)
       }
    }

}

teste = new heroi("Tester", 32, "ninja")
console.log(teste)
teste.atacar()
class Carros{

    private modelo: string
    public apelido: string 
    private numeroDePorta: number
    private velocidade: number = 0

    constructor(modelo: string,numeroDePorta: number){
        this.modelo=modelo;
        this.apelido = "";
        this.numeroDePorta=numeroDePorta;
    }

    public acelerar(): void{
        this.velocidade = this.velocidade+10;
    }

    public dizerModelo(): string{
        return this.modelo;
    }

    public parar(): void{
        this.velocidade = 0
    }

    public velocidadeAtual(): number{
        return this.velocidade;
    }
   
}

class Concessionarias{

    private endereco : string
    private listaDeCarros: Array<Carros>

    constructor(endereco : string,listaCarros: Array<Carros>){
        this.endereco = endereco;
        this.listaDeCarros = listaCarros;
    }

    public fornecerEndereco(): string{
        return this.endereco;
    }

    public mostrarListaDeCarros(): Array<Carros>{
        return this.listaDeCarros;
    }

}

class Pessoas {

    private nome: string
    private carroPreferido: string
    private carro : any
    
    constructor(nome : string,carroPreferido: string){
        this.nome = nome;
        this.carroPreferido = carroPreferido;
    }

    public dizerNome(): string{
        return this.nome;
    }

    public dizerCarroPreferido(): string{
        return this.carroPreferido;
    }

    public comprarCarro(carro : any): void{
       this.carro = carro;
    }

}

let carroA = new Carros('Meriva',4);
let carroB = new Carros('Agile',4);
let carroC = new Carros('Celta',4);

/* carros concessionaria */
let listaCarros : Carros[] = [carroA,carroB,carroC];
//let listaCarros : Array<Carro> = [carroA,carroB,carroC];

carroA.acelerar();

console.log(carroA);
carroA.acelerar();
carroA.apelido = 'Poçante';
console.log(carroA);


let concessionaria = new Concessionarias('Avenida Faria Lima',listaCarros);
console.log(concessionaria);
concessionaria.mostrarListaDeCarros();



let pessoa = new Pessoas("Fernando Tugu","Corolla");
pessoa.dizerNome();
pessoa.dizerCarroPreferido();
pessoa.comprarCarro(carroA);
console.log(pessoa);


let cliente = new Pessoas("Roberto","Celta");

console.log('Cliente');
console.log(cliente);

concessionaria.mostrarListaDeCarros().map((carro: Carros)=>{
    if(carro.dizerModelo() == cliente.dizerCarroPreferido()){
        cliente.comprarCarro(carro);
    }
    //console.log(carro);
})
console.log(cliente);
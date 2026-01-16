import app from "./app"

// - Exercício 1
    
//     Analise a classe `UserAccount` abaixo. Perceba quais propriedades são públicas e quais são privadas e responda as perguntas discursivas em comentários no arquivo `index.ts`
    
//     a) *Para que serve o construtor dentro de uma classe e como fazemos para chamá-lo?*
    
//     b) *Copie o código abaixo para o seu exercício de hoje; crie uma instância dessa classe (dê o nome, cpf e idade que você quiser). Quantas vezes a mensagem `"Chamando o construtor da classe User"` foi impressa no terminal?*
    
//     c) *Como conseguimos ter acesso às propriedades privadas de uma classe?*

class UserAccount {
    private cpf: string;
    private name: string;
    private age: number;
    private balance: number = 0;
    private transactions: Transaction[] = [];
  
    constructor(
       cpf: string,
       name: string,
       age: number,
    ) {
       console.log("Chamando o construtor da classe UserAccount")
       this.cpf = cpf;
       this.name = name;
       this.age = age;
    }
  
  }


//   - Exercício 2
    
//     Transforme a variável de tipo abaixo, chamada `Transaction` em uma classe `Transaction`. 
//     Ela deve conter as mesmas propriedades: *data*, *valor* e *descrição*.
//      Agora, porém, todas elas devem ser **privadas**. Portanto, crie métodos (*getters*) para a leitura desses valores,
//      tanto para essa classe quanto para a classe UserAccount.
//      Crie uma instância dessa classe e adicione à instância já criada de UserAccount
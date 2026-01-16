import { mergeObjects, mergeArrays, extractFields, getPessoaCallback, getPessoaPromisse } from '../0-exercicios/exercicios';

/************************************
 * 
 * EXERCÍCIOS DO 1º DIA
 * 
 **************************************
*/

/** Exercício 1.
 * Criar uma função que receba como argumento dois objetos e retorne um terceiro objeto
 * Que contenha todas as chaves e os valores dos dois */
test('Merge Objects', () => {

    const contact = {
        nome: 'Joaldo',
        email: 'joaldo@hotmail.com'
    }

    const address = {
        rua: 'Java',
        numero: 8
    }

    expect(mergeObjects(contact, address)).toEqual({ ...contact, ...address });
});


/** Exercício 2.
* Criar uma função que receba 2 arrays como argumento e retorne 
* Um terceiro com os elementos dos dois arrays (sem utilizar o concat e spread) */
test('Merge Arrays', () => {

    const contact = [
        { nome: 'Joaldo', email: 'joaldo@hotmail.com' },
        { nome: 'Dunha', email: 'dunha@hotmail.com' }
    ]

    const address = [
        { rua: 'Java', numero: 8 }
    ]

    expect(mergeArrays(contact, address)).toEqual([...contact, ...address]);
});

/** Exercício 3.
 *  Destructuring de um objeto de domínio da aplicação que trabalha, extraindo propriedades de dois níveis */

test('Destructuring', () => {

    const pessoa = {
        nome: 'Joaldo',
        email: 'joaldo@hotmail.com',
        endereco: {
            rua: 'Java',
            numero: 8
        }
    }

    const { endereco: { numero: numeroExpected } } = pessoa

    expect(numeroExpected).toEqual(8);
});


/** Exercício 4.
 *  Criar uma função que recebe um objeto complexo e um array de campos que serão extraídos */
test('Extract fields then object', () => {

    const pessoa = {
        nome: 'Joaldo',
        telefone: '000-000-000',
        endereco: 'Rua Java'
    }

    const resultExpected = {
        nome: 'Joaldo',
        telefone: '000-000-000'
    }

    expect(extractFields(pessoa, 'nome', 'telefone')).toEqual(resultExpected);
});

/************************************
 * 
 * EXERCÍCIOS DO 2º DIA
 * 
 **************************************
 * Exercício 1.
 * 
 * Criar uma função que simule uma requisição http para uma API REST (seja criativo).
 * Essa função deverá receber outras duas como argumento: uma função em caso de sucesso e outra em caso de falha.
 * De de acordo com o comportamento da sua api a função de sucesso ou falha deverá ser invocada.
 * 
 * Caso o requisição seja feita no /jstraining/api/#onomedasuaapi então deve retornar um json com o resultado que você quiser; Caso contrário um erro deverá ser retornado.
 * 
 * Obs
 *  - Não precisa colocar a asserção dos testes, apenas simular o comportamento de uma requisição normal.
 *  - Você deve chamar a sua função dentro do bloco de testes abaixo 'http mock with callbacks'
 */
test('http mock with callbacks', () => {

    const functionSuccess = pessoa => console.log(
        `O ${pessoa.nome} conseguiu fazer uma função que recebee callbacks!`)
    const functionError = error => console.log(error)

    getPessoaCallback('/api/pessoa', functionSuccess, functionError);
});

/**
 * Exercício 2.
 * 
 * Chamar a função que foi criada duas vezes, garantindo a sua ordem de execução.
 *
 */
test('http mock with callbacks chaining', () => {

    const functionSuccess = pessoa => {
        console.log(`O ${pessoa.nome} conseguiu fazer uma função que recebee callbacks!`);

        getPessoaCallback('/teste', s => console.log(s), e => console.log(e));
    };
    const functionError = error => console.log(error)

    getPessoaCallback('/api/pessoa', functionSuccess, functionError)
});

/**
 * Exercício 3
 * 
 * Refatorar o exercício dos callbacks (#2) para utilizar Promises conforme exemplo mostrado anteriormente
 */
test('refactoring callback exercise', () => {

    const resolve = pessoa => console.log(
        `O ${pessoa.nome} conseguiu fazer uma função que recebee callbacks!`)
    const reject = error => console.log(error)

    getPessoaPromisse('/api/pessoa')
        .then(v => resolve(v))
        .then(() => done())
        .catch(result => console.log(result))
        .then(() => done());
});

/**
 * Exercício 4
 * 
 * Refatorar o exercício do encadeamento dos callbacks para utilizar o encadeamento das promises.
 */
test('refactoring callback chaining promises', () => {

    getPessoaPromisse('/api/pessoa')
        .then(result => {
            console.log(result)
            return result.nome;
        })
        .then(result => {
            console.log(result)
        })
        .then(() => getPessoaPromisse('api/erro')
            .then(() => done())
            .catch(result => console.log(result))
            .then(() => done()));
});
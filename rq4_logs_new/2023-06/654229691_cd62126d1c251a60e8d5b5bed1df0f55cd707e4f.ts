function informarDadosPessoais(idPessoa : number, nome : string, email?: string){
    console.log('idFuncionario...:  ', idPessoa, 'Nome... : ' , nome)

    if(email !== undefined) console.log('Email... : ' , email);
}

informarDadosPessoais(775544, 'Iagatha');
informarDadosPessoais(994411, 'Eliza', 'eliza@gmail.com');

// exemplo 2

function mensagemLog(mensagem : string, usuarioId ?: number){
    const horaLog = new Date().toLocaleTimeString();

    console.log(horaLog, mensagem, usuarioId || 'Usuario não conectado');
}

mensagemLog('Atualizar Página');
mensagemLog('Usuário logado com sucesso', 778811);

// exemplo 3

/*type Pessoa = {
    idFuncionario : number;
    nome : string;
    idade ?: number;
    email ?: string;
};

let pessoa : Pessoa;

pessoa = {
    idFuncionario : 112233,
    nome : 'Iagatha'
}; 
*/
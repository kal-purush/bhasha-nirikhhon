const conexao = require('../../config/conexao/conexaoDatabase');

module.exports = (cpf, callbackVerificado) => {
    
    const sqlCpf = `SELECT * FROM usuario WHERE cpf=?`

    conexao.query(sqlCpf, cpf, (erro, resultadoUsuarioCpf) => {
        const usuarioCpf = resultadoUsuarioCpf[0];
        if(erro){
            console.log(erro);
        }else if(usuarioCpf){
            const cpfExiste = true;
            callbackVerificado(cpfExiste);   
        }else{
            const cpfExiste = false;
            callbackVerificado(cpfExiste);  
        }
    });
}
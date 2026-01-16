function validateForm() {
    // Verifica se o campo 'nome' contém algum número
    var nome = document.getElementById('nome').value;
    if (/[\d]+/.test(nome)) {
        alert("O nome não deve conter números.");
        return false;
    }

    // Verifica se o campo 'email' está vazio ou se não contém um '@'
    var email = document.getElementById('email').value;
    if (email === "" || !email.includes('@')) {
        alert("Por favor, insira um endereço de email válido.");
        return false;
    }

    // Verifica se o campo 'telefone' está vazio
    var telefone = document.getElementById('telefone').value;
    if (telefone === "") {
        alert("Por favor, insira um número de telefone.");
        return false;
    }

    // Verifica se o campo 'mensagem' está vazio
    var mensagem = document.getElementById('mensagem').value;
    if (mensagem.trim() === "") {
        alert("Por favor, escreva uma mensagem.");
        return false;
    }

    var dadosFormulario = {
        nome: nome,
        email: email,
        telefone: telefone,
        mensagem: mensagem,
        assunto: document.getElementById('assunto').value,
        dataEnvio: new Date().toLocaleString()
    };

    // Armazenar dados no localStorage
    localStorage.setItem('dadosFormulario', JSON.stringify(dadosFormulario));

    // Redirecionar para a nova página
    window.location.href = 'pagina-de-dados.html';

    // Impede o envio real do formulário
    return false;
}
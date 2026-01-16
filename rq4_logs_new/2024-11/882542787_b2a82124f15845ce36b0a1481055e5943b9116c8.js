document.addEventListener("DOMContentLoaded", function() {  // Garante que o código dentro seja executado apenas após todo o documento HTML ter sido completamente carregado e analisado
    const formContato = document.getElementById("formulario-contato"); // Seleciona o formulário de contato pelo seu ID para poder adicionar um evento de envio

    formContato.addEventListener("submit", function(event) { // Adiciona um evento de envio ao formulário
        event.preventDefault(); // Impede que o formulário recarregue a página ao ser enviado

        // Coleta os valores dos campos de entrada do formulário
        const nome = document.getElementById("nome").value;
        const telefone = document.getElementById("telefone").value;
        const email = document.getElementById("email").value;
        const mensagem = document.getElementById("mensagem").value;

        // Parâmetros para enviar o e-mail, com os valores coletados dos campos de entrada
        const templateParams = {
            from_name: nome,
            from_telefone: telefone, 
            from_email: email,
            message: mensagem
        };

        // Envia o e-mail usando EmailJS - serviço gratuito para envio de e-mails
        emailjs.send('service_8ccptl9', 'template_68963hv', templateParams)
            .then(function(response) { // Caso o envio seja bem-sucedido, exibe uma mensagem de sucesso
                console.log('SUCCESS!', response.status, response.text);
                document.getElementById("mensagem-sucesso").style.display = "block";
                document.getElementById("mensagem-erro").style.display = "none";
                formContato.reset();

            }, function(error) { // Caso o envio falhe, exibe uma mensagem de erro
                console.log('FAILED...', error);
                document.getElementById("mensagem-erro").style.display = "block";
                document.getElementById("mensagem-sucesso").style.display = "none";
            });
    });
});
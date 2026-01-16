// Verifica se o DOM está carregado antes de executar
document.addEventListener('DOMContentLoaded', function () {
    // Define a função de alternância
    window.toggleHistoria = function () {
        const conteudo = document.getElementById('conteudoHistoria');
        const button = document.getElementById('toggleHistoria');

        if (conteudo.style.display === 'none' || conteudo.style.display === '') {
            conteudo.style.display = 'block'; // Exibe o conteúdo
            button.textContent = 'Minimizar'; // Atualiza o texto do botão
        } else {
            conteudo.style.display = 'none'; // Oculta o conteúdo
            button.textContent = 'Maximizar'; // Atualiza o texto do botão
        }
    };
});
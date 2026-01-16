const campos = document.querySelectorAll('.campo');
const jogador1 = document.querySelector('#jogador1');
const jogador2 = document.querySelector('#jogador2');
const info = document.querySelector('#informacoes');
let jogada = 0;
const camposX = [];
const camposO = [];
const casosDeSucesso =
    [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
        [1, 4, 7],
        [2, 5, 8],
        [3, 6, 9],
        [1, 5, 9],
        [3, 5, 7]
    ];

jogador1.textContent = verificaPlacar('jogador1');
jogador2.textContent = verificaPlacar('jogador2');

campos.forEach(campo => {
    campo.addEventListener('click', () => {
        if (!(jogada % 2)) {
            campo.textContent = 'X';
            camposX.push(parseInt(campo.getAttribute("value")));
            if (verificaVitoria(camposX)) {
                aumentaPlacar('jogador1');
                jogador1.textContent = verificaPlacar('jogador1');
                reiniciaOJogo();
            }else if(jogada == 8) {
                info.textContent = 'Deu velha!';
                info.classList.add('empate');
                reiniciaOJogo();
            };
        }
        else {
            campo.textContent = 'O';
            camposO.push(parseInt(campo.getAttribute("value")));
            if (verificaVitoria(camposO)) {
                aumentaPlacar('jogador2');
                jogador2.textContent = verificaPlacar('jogador2');
                reiniciaOJogo();
            }
        }
        console.log(jogada);
        jogada++;
    })
});

function marcaCamposVitorioso(arrayVitorias) {
    for(campoVitorioso of arrayVitorias){
        campos.forEach(campo => {
            if(campo.getAttribute("value") == campoVitorioso) campo.classList.add('vencedor');
        })
    }
}

function verificaVitoria(arrayJogadas) {

    const vitorias = casosDeSucesso
        .filter(casos => {
            let contador = 0;
            for (caso of casos) {
                if (arrayJogadas.includes(caso)) contador++;
            }
            if (contador === 3) return true;
            return false;
        })
        .reduce((arr, item) => arr.concat(item), []);

    if (vitorias.length) {
        marcaCamposVitorioso(vitorias);
        return true;
    }
    return false;
}

function verificaPlacar(placarDoJogador) {
    let resultado = sessionStorage.getItem(placarDoJogador);
    if (resultado === null) {
        sessionStorage.setItem(placarDoJogador, '0');
        resultado = sessionStorage.getItem(placarDoJogador);
    };
    return parseInt(resultado);
}

function aumentaPlacar(placarDoJogador) {
    sessionStorage.setItem(placarDoJogador, verificaPlacar(placarDoJogador) + 1);
}

function reiniciaOJogo() {
    setTimeout(() => {
        campos.forEach(campo => {
            campo.textContent = ''
            campo.classList.remove('vencedor');
        });
        info.classList.remove('empate');
        info.textContent = '';
        jogada = 0;
    }, 1500);
    while (camposX.length) camposX.pop();
    while (camposO.length) camposO.pop();
}


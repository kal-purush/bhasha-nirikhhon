const form = document.getElementById('form');

form.addEventListener('submit', handleSubmit);

function handleSubmit(event) {
    event.preventDefault();

    const genero = getSelectedValue('genero');
    const idade = converterNumber('idade');
    const peso = converterNumber('peso');
    const altura = converterNumber('altura');
    const atividade = getSelectedValue('atividade');

    const taxa = (
        genero === 'feminino'
            ? Math.round(655 + (9.6 * peso) + (1.8 * altura) - (4.7 * idade))
            : Math.round(66 + (13.7 * peso) + (5 * altura) - (6.8 * idade))
    );

    const manutencao = Math.round(taxa * Number(atividade));
    const reducao = (manutencao - 450);
    const ganha = (manutencao + 450);

    const layout = `
    <h2>Aqui está o resultado:</h2>

    <div class="result-content">
      <ul>
        <li>
          Sua taxa de metabolismo basal é de <strong>${taxa} calorias</strong>.
        </li>
        <li>
          Para manter o seu peso você precisa consumir em média <strong>${manutencao} calorias</strong>.
        </li>
        <li>
          Para perder peso você precisa consumir em média <strong>${reducao} calorias</strong>.
        </li>
        <li>
          Para ganhar peso você precisa consumir em média <strong>${ganha} calorias</strong>.
        </li>
      </ul>
    </div>
    `;

    const result = document.getElementById('result');

    result.innerHTML = layout;
}



function getSelectedValue(id) {
    const select = document.getElementById(id);
    return select.options[select.selectedIndex].value;
}

function converterNumber(id) {
    return Number(document.getElementById(id).value);
}

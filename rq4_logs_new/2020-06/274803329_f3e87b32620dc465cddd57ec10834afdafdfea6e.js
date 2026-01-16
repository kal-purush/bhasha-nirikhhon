var btnAdiciona = document.querySelector('#buscar-pacientes');

btnAdiciona.addEventListener('click', function (event) {

    var xhr = new XMLHttpRequest();

    xhr.open('GET', 'http://api-pacientes.herokuapp.com/pacientes');

    xhr.addEventListener('load', function () {
        var erroAjax = document.querySelector('#erro-ajax');

        if (xhr.status == 200) {
            erroAjax.classList.add('invisivel');

            var resposta = xhr.responseText;

            var pacientes = JSON.parse(resposta);

            pacientes.forEach(paciente => {
                adionaPacienteNaTabela(paciente)
            });
        } else {
            console.log(xhr.status);
            console.log(xhr.responseText);
            
            erroAjax.classList.remove('invisivel');
        }
    })

    xhr.send();
})
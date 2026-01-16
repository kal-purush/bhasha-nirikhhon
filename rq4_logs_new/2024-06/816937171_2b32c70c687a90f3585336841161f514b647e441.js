function fetchCursos() {
    fetch('/api.php?entity=cursos')
        .then(response => response.json())
        .then(data => {
            const listaCursos = document.getElementById('lista-cursos');
            listaCursos.innerHTML = '';
            data.forEach(curso => {
                const div = document.createElement('div');
                div.classList.add('curso');
                div.innerHTML = `
                    <img src="images/${curso.codigo_unico}.jpg" alt="${curso.nome}">
                    <h3>${curso.nome}</h3>
                    <p>${curso.descricao}</p>
                    <p>Carga Horária: ${curso.carga_horaria} horas</p>
                    <button onclick="inscrever(${curso.id})">Inscrever-se</button>
                `;
                listaCursos.appendChild(div);
            });
        });
}
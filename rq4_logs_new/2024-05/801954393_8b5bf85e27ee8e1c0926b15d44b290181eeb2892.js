document.getElementById('vacancy-form').addEventListener('submit', function(event) {
    event.preventDefault();
    const vacancy = document.getElementById('vacancy').value;
    const resumeOutput = document.getElementById('resume-output');

    // Здесь будет логика для персонализации резюме
    resumeOutput.innerHTML = `<p>Вы ввели вакансию: ${vacancy}</p>`;
});
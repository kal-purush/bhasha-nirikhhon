const form = document.querySelector('body');
form.addEventListener('submit', function(e) {
    e.preventDefault();
    const height = parseInt(document.querySelector('#height').value);
    const weight = parseInt(document.querySelector('#weight').value);
    const results = document.querySelector('#results');
    if (height === '' || height < 0 || isNaN(height)) {
        results.innerHTML = `Please give a valid height ${height}`;
    } else if (weight === '' || weight < 0 || isNaN(weight)) {
        results.innerHTML = `Please give a valid weight ${weight}`;
    } else {
        const bmi = (weight / ((height * height) / 10000)).toFixed(2);

        results.innerHTML = `<span>${bmi}</span>`;
        if (`${bmi}` <= 18.6) {
            results1.innerHTML = `underweight`;
        } else if (24.9 > `${bmi}` > 18.6) {
            results1.innerHTML = `Normal Weight`;
        } else if (`${bmi}` > 24.9) {
            results1.innerHTML = `Overweight`;
        }

    }

});
const form = document.querySelector('form');

form.addEventListener('submit', function (e) {
    e.preventDefault();

    const height = parseInt(document.querySelector('#height').value)
    const weight = parseInt(document.querySelector('#weight').value)
    const results = document.querySelector('#result')


    console.log(height);
    console.log(weight);

    if (height === '' || height < 0 || isNaN(height)) {
        results.innerHTML = `Please give a valid height ${height}`;
    } else if (weight === '' || weight < 0 || isNaN(weight)) {
        results.innerHTML = `Please give a valid weight ${weight}`;
    } else {
        const bmi = (weight / ((height * height) / 10000)).toFixed(2);
        if (bmi <= 18.60) {
            results.innerHTML = `<span>${bmi}</span><br> <p><b>You are Underweight`;
        } else if (bmi <= 24.90) {
            results.innerHTML = `<span>${bmi}</span><br> <p><b>Your BMI is normal`;
        } else {
            results.innerHTML = `<span>${bmi}</span><br> <p><b>You are Overweight`;
        }

    }




});
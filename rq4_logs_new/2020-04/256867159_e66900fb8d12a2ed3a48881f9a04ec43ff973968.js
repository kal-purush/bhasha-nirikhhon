function buttonOver() {
    let button = document.querySelector('#confirm');

    button.style.cursor = 'pointer';
};

function currentAge(birthYear, birthMounth, birthDay) {
    var day = new Date;

    let currentDay = day.getDate();
    let currentMounth = day.getMonth();
    let currentYear = day.getFullYear();

    var birthDay = +birthDay;
    var birthMounth = +birthMounth;
    var birthYear = +birthYear;

    var current_age = currentYear - birthYear;

    if (currentMounth < birthMounth || currentMounth == birthMounth && currentDay < birthDay) {
        current_age--;
    };

    return current_age < 0 ? 0 : current_age;
}

function maiorIdade() {

    var birth = document.querySelector('.pais');

    var age = birth.value;

    var diferença = currentAge() - age;
    alert(diferença);
};
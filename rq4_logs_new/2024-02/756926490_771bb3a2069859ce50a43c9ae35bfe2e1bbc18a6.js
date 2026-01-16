function hideElement(hideHomeScreen) {
    const element = document.getElementById(hideHomeScreen);
    // console.log (element);
    element.classList.add('hidden');
}
function showElement(showPlayNow) {
    const showPlayNowScreen = document.getElementById(showPlayNow);
    showPlayNowScreen.classList.remove('hidden');
}


function getARandomAlphabet() {
    const alphabetString = 'abcdefjhijklmnopqrstuvwxyz';
    const alphabets = alphabetString.split('');
    // console.log (alphabets)

    const randomNumber = Math.random()*25;
    const index = Math.round(randomNumber);
    // console.log (index);

    const alphabet = alphabets[index];
    // console.log(alphabet, index);
    return alphabet;
}
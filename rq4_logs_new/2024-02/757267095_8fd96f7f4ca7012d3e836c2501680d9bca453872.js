// function play(){
//     // hight the home screen. to hide the screen add the class hidden to the home section 

//     const homeSection = document.getElementById('home-screen');
//     homeSection.classList.add('hidden');

//     const playgroundSection = document.getElementById('play-ground');
//     playgroundSection.classList.remove('hidden')
    
// }

function continueGame(){
    //step -1: generate a ransom alphabet
    const alphabet = getARandomAlphabet();
    console.log('you', alphabet);

    const currentAlphabetElement = document.getElementById('current-alphabet');
    currentAlphabetElement.innerText = alphabet;

    // stet background color 

    setBackgroundColorById(alphabet);
}

function play(){
    hideElementById('home-screen');
    showElementById('play-ground');
    continueGame()
}

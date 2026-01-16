const rng = document.querySelectorAll('.color-range')
const rgbColorCode = document.querySelectorAll('.rgb-color-code');
const hexCode = document.querySelectorAll('.hex-color-code');
const circleDisplay = document.getElementById('circleDisplay')
const rVar = document.getElementById('redVariant');
const gVar = document.getElementById('greenVariant');
const bVar = document.getElementById('blueVariant');


for (let i=0; i<=rng.length-1; i++){
        rgbColorCode[i].innerHTML =parseInt(rng[i].value);
        hexCode[i].innerHTML = parseInt(rng[i].value).toString(16);
        circleDisplay.style.background = `
        rgb(${rng[0].value}, ${rng[1].value}, ${rng[2].value})`;

        rVar.style.background = `rgb(${rng[0].value}, 0, 0)`;
        gVar.style.background = `rgb(0, ${rng[1].value}, 0)`;
        bVar.style.background = `rgb(0,0, ${rng[2].value})`;
      
}

for (let i=0; i<=rng.length-1; i++){
    rng[i].oninput = () => {
        rgbColorCode[i].innerHTML = rng[i].value;
        hexCode[i].innerHTML = parseInt(rng[i].value).toString(16);
        circleDisplay.style.background = `
        rgb(${rng[0].value}, ${rng[1].value}, ${rng[2].value})`;
        
        // shades of RED, GREEN and BLUE
        rVar.style.background = `rgb(${rng[0].value}, 0, 0)`;
        gVar.style.background = `rgb(0, ${rng[1].value}, 0)`;
        bVar.style.background = `rgb(0,0, ${rng[2].value})`;

        
    }
}
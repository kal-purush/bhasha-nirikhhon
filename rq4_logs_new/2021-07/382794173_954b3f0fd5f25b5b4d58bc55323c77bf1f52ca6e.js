"use strict"

import Calculs from "./classes/Calculs.js";
// import dataTranslation from "./classes/Trads.js";
import french from "../lang/french.js";
import english from "../lang/english.js";
let languages = {"french": french, "english": english};

const DIFFUSION_TIME_IN_BLOOD = 4; // time in hour
const OPTIMAL_ELIMINATION_TIME = 4; //time in hour

let divAgreement = document.querySelector('#agreement');
let divAgreementHalfLife = document.querySelector('#container_calc_half_life');
let divCalcTox = document.querySelector('#calc_tox');
let divNeedDose = document.querySelector('#need_dose');
let divMsgError = document.querySelector('.alertBadData');
let divResult = document.querySelector('#container_result');

let labelAgreement = document.querySelector('.text_agreement');
let labelNeedHalfLife = document.querySelector('.text_need_half_life');
let labelFirstSampleDatePicker = document.querySelector('.first_sampling_label');
let labelSecondSampleDatePicker = document.querySelector('.second_sampling_label');

let mainTitle = document.querySelector('.main_title');
let subTitle = document.querySelector('.sub_title');

let pResult = document.querySelector('.p_result');
let pResultNeedDose = document.querySelector('#need_dose_result');
let pResultCalcHalfLife = document.querySelector('#result_calc_half_life');

let btnTranslation = document.querySelectorAll('.btn_translation');
let btnCalcHalfLife = document.querySelector('.calc_half_life');
let btnSubmit = document.querySelector('.calc_data');
let btnCalcDose = document.querySelector('.calc_dose_button');

let datePickerFirstSample = document.querySelector('.first_sampling_time');
let datePickerSecondSample = document.querySelector('.second_sampling_time');

let checkBoxAgreements = document.querySelectorAll('input[type="checkbox"]');

let inputIngestioTime = document.querySelector('.time_after_ingestion');
let inputParacetamolConcentration = document.querySelector('.paracetamol_concentration');
let inputParacetamolFirstSample = document.querySelector('.paracetamol_concentration_1');
let inputParacetamolSecondSample = document.querySelector('.paracetamol_concentration_2');
let inputHypotheticDose = document.querySelector('.subject_hypothetic_dosis');
let inputSubjectWeight = document.querySelector('.subject_weight');

let graphCanvas = document.querySelector('.graph');
let timeAfterIngestion;
let paracetamolConcentration;
let currentLanguage = "french";

// ##################
let graph = new Chart(graphCanvas, {
    data: 
    {
        datasets: 
        [
            {
            type: 'line',
            label: languages[currentLanguage].graph.toxLine,
            labelName: "toxLine",
            borderColor: 'rgb(75, 192, 192)',
            backgroundColor: 'rgb(75, 192, 192)',
            data: [,200,100,50,25,12.5,6.25]
            },

            {
            type: 'line',
            label: languages[currentLanguage].graph.secondLine,
            labelName: "secondLine",
            borderColor: 'brown',
            backgroundColor: 'brown',
            data: [,150,75,37.5,18.75,9.375,4.6875],
        },
        
        {
            type: 'bubble',
            label: languages[currentLanguage].graph.patientSaisi,
            labelName: "patientSaisi",
            fill: true,
            backgroundColor: 'rgb(255, 99, 132)',
            data: [{}]
            }
        ],
        labels: [0,4,8,12,16,20,24]
    },

    options: 
    {
        responsive: true,
        plugins: 
        {
            title: 
            {
                display: true,
                text: languages[currentLanguage].graph.title,
                textName: "title"
            }
        },

        scales: 
        {
            x: 
            {
                display: true,
                min:0,
                max:25,
                type: 'linear'
            },

            y: 
            {
                min:0,
                max:600,
                display: true,
                type: 'logarithmic'
            }
        }
    }
});

// on initialise avec le texte de la page en français
changeLanguage("french");


// Eventlistener definitions
btnSubmit.addEventListener("click", () => {
    paracetamolConcentration = parseFloat(inputParacetamolConcentration.value);
    timeAfterIngestion = checkValidity(parseFloat(inputIngestioTime.value)); 

    displayResult(timeAfterIngestion, paracetamolConcentration );
    addData(graph, timeAfterIngestion, paracetamolConcentration );
    pResult.scrollIntoView(true);
})

// Chaque checkbox toggle l'affichage de la div qui lui correspond
checkBoxAgreements.forEach(checkbox => {
    checkbox.addEventListener("click", (event) => {
        if(event.target.checked == true) {
            document.querySelector('#' + event.target.dataset.div).classList.remove("invisible");
            document.querySelector('#' + event.target.dataset.div).classList.add("visible");
        } else {
            document.querySelector('#' + event.target.dataset.div).classList.remove("visible");
            document.querySelector('#' + event.target.dataset.div).classList.add("invisible");
        }
    })
});

// btnTranslation.forEach( btn => {
//     btn.addEventListener("click", () => {
//         currentLangage = btn.dataset.langage;
//         graph.data.datasets.forEach( dataset => {
//             dataset.label = dataTranslation[currentLangage].graph[dataset.labelName];
//         })
//         graph.options.plugins.title.text = dataTranslation[currentLangage].graph[graph.options.plugins.title.textName];
//         graph.update();
//         changeLangage();
//     })
// });

btnCalcHalfLife.addEventListener("click", () => {
    displayHalfLife();
})

btnCalcDose.addEventListener("click", () => {
    displayDoseWeight();
})


// function definitions
// function changeLangage() {

//     labelAgreement.textContent = dataTranslation[currentLangage].buttons.checkboxLabelAgreement;
//     labelNeedHalfLife.textContent = dataTranslation[currentLangage].buttons.checkboxLabelHalfLife;
//     labelFirstSampleDatePicker.textContent = dataTranslation[currentLangage].buttons.datePickerLabelFirstSample;
//     labelSecondSampleDatePicker.textContent = dataTranslation[currentLangage].buttons.datePickerLabelSecondSample;
    
//     inputIngestioTime.placeholder = dataTranslation[currentLangage].buttons.ingestionTimePlaceholder;
//     inputParacetamolConcentration.placeholder = dataTranslation[currentLangage].buttons.paracetamolplaceholder;
    
//     mainTitle.textContent = dataTranslation[currentLangage].title.main;
//     subTitle.textContent = dataTranslation[currentLangage].title.subTitle;
//     btnSubmit.textContent = dataTranslation[currentLangage].buttons.btnValidation;
// }

btnTranslation.forEach(btn => {
    btn.addEventListener('click', () => {
        let language = btn.dataset.language;
        changeLanguage(language)
    })
})

function changeLanguage(language) {
    document.querySelectorAll(".i18n").forEach( element => {
        let textID = element.dataset.text;
        if(element.placeholder != null && element.dataset.text != null) {
            element.placeholder = languages[language][textID];
        } else if(element.dataset.text != null) {
            element.innerText = languages[language][textID];
        } 
        
    })
}

function addData(chart, time, concentration) {
    chart.data.datasets[2].data[0]= {x:time,y:concentration,r:7};
    chart.update();
}

function checkValidity(number) {
    if(number < DIFFUSION_TIME_IN_BLOOD) {
        setNewAttribute(divMsgError, "visible");
        setNewAttribute(divResult, "invisible");
        return null;
    } else {
        setNewAttribute(divMsgError, "invisible");
        return number;
    }
}

function setNewAttribute(htmlComponent, classAttribute) {
    htmlComponent.setAttribute("class", classAttribute);
}


function displayHalfLife() {
    let valeur1 = parseFloat(inputParacetamolFirstSample.value);
    let valeur2 = parseFloat(inputParacetamolSecondSample.value);
    let duree = (Date.parse(datePickerSecondSample.value) - Date.parse(datePickerFirstSample.value)) / 3_600_000;


    if(datePickerSecondSample.value > datePickerFirstSample.value && valeur1 > valeur2) {
        let result = Calculs.calcHalfLife(valeur1, valeur2, duree);
        let resultIsOk = checkHalfLifeResult(result);
        displayResultHalfLife(resultIsOk);
        pResultCalcHalfLife.textContent = `${dataTranslation[currentLangage].results.resultHalfLife} ${result} h`;    
    } else {
        displayResultHalfLife(false);
        pResultCalcHalfLife.textContent = dataTranslation[currentLangage].results.badCalcul;
    }
}


function displayResult(time, concentration) {
    let toxicity =  Calculs.calcToxicity(time);
    let toxicityPossible = Calculs.calcToxicityPossible(toxicity);
    let toxicityProbable = Calculs.calcToxicityProbable(toxicity);

    if(concentration > toxicityProbable) {
        setNewAttribute(divResult, "visible");
        pResult.textContent = dataTranslation[currentLangage].results.resultProbable;
    } else if (concentration > toxicityPossible) {
        setNewAttribute(divResult, "visible");
        pResult.textContent = dataTranslation[currentLangage].results.resultPossible ;
    } else if (timeAfterIngestion != null) {
        setNewAttribute(divResult, "visible");
        pResult.textContent = dataTranslation[currentLangage].results.resultOk;
    }
}
// ##########################################

// ##########################
function displayResultHalfLife(resultIsOk) {
    if(resultIsOk) {
        setNewAttribute(pResultCalcHalfLife, "visible result_ok");
    } else {
        setNewAttribute(pResultCalcHalfLife, "visible result_not_ok");
    }
}

function checkHalfLifeResult(number) {
    if(number <= OPTIMAL_ELIMINATION_TIME) {
        return true;
    } else {
        return false;
    }
}


function displayDoseWeight() {
    let weight = inputSubjectWeight.value;
    let dose = inputHypotheticDose.value;
    let result = dose * 1000 / weight;
    pResultNeedDose.textContent = `La dose ingérée est donc ${result.toFixed(0)} mg/kg.`;
}

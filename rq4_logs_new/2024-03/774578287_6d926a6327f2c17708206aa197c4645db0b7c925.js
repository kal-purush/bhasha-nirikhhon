var translations = {
    "contact": {
        "en": "CONTACT",
        "pl": "KONTAKT"
    },
    "daneOsobowe": {
        "en": "PERSONAL DETAILS",
        "pl": "DANE OSOBOWE"
    },
    "dateOfBirth": {
        "en": "Date of birth 18.09.2000",
        "pl": "Data urodzenia 18.09.2000"
    },
    "adres": {
        "en": "Address 1 Franciszka Bohomolca 11/85, Małopolska 31-416 Kraków",
        "pl": "Adres 1 Franciszka Bohomolca 11/85, Małopolska 31-416 Kraków"
    },
    "languages": {
        "en": "LANGUAGES",
        "pl": "JĘZYKI"
    },
    "polish": {
        "en": "Polish (native)",
        "pl": "Polski (ojczysty)"
    },
    "english": {
        "en": "English B2",
        "pl": "Angielski B2"
    },
    //RIGHT SIDE HEADERS
    "abilities": {
        "en": "QUALITIES",
        "pl": "ZALETY"
    },
    "skills": {
        "en": "SKILLS",
        "pl": "UMIEJĘTNOSCI"
    },
    "experience": {
        "en": "EXPERIENCE/ EDUCATION",
        "pl": "DOŚWIADCZENIE/ EDUKACJA"
    },
    "certyfications": {
        "en": "CERTYFICATES/ COURSES/ TRAININGS",
        "pl": "CERTYFIKATY/ KURSY/ SZKOLENIATY"
    },
    // RIGHT SIDE CONTENT
    "creativity": {
        "en": "Creative",
        "pl": "Kreatywność"
    },
    "reliable": {
        "en": "Reliable",
        "pl": "Rzetelność"
    },
    "communication": {
        "en": "Communication",
        "pl": "Umiejętność pracy w grupie"
    },
    "innovation": {
        "en": "Innovative",
        "pl": "Innowacyjność"
    },
    "additionalSkills": {
        "en": "Additionally: Spring, Spring Boot, Bootstrap, JUnit, GIT, Maven, Gradle, Design Patterns, SOLID, API",
        "pl": "Dodatkowo: Spring, Spring Boot, Bootstrap, JUnit, GIT, Maven, Gradle, Wzorce projektowe, SOLID, API"
    },
    //EXPERIENCE
    "middleSchool": {
        "en": "Stanisław Staszic IT Technical School in Staszow",
        "pl": "Nauka w technikum inforamtycznym im.Stanisława Staszica w Staszowie"
    },
    "practice": {
        "en": "Erasmus Inthernship at FlyPhone in Spain",
        "pl": "Praktyki w ramach Erasmus w firmie FlyPhone/ Hiszpania"
    },
    "work": {
        "en": "Połaniec Elporem Power Plant and Elpoautomatyka company z.o.o. Automation Technican",
        "pl": "Elektrownia Połaniec Eloprem i Elpoautomatyka company z.o.o Stanowisko automatyk"
    },
    "current": {
        "en": "Individual studing programming under the supervision of a mentor. Learning in progress.",
        "pl": "Nauka programowania pod okiem mentora. W trakcie nauki"
    },
    //CERTIFICATES
    "cert1": {
        "en": "E13 - Design of local area networks and networks administration.",
        "pl": "E13 - Projektowanie lokalnych sieci komputerowych i administrowanie sieciami "
    },
    "cert2": {
        "en": "E14 - Developing web and databases applications and database administration.",
        "pl": "E14 - Tworzenie aplikacji internetowych i baz danych oraz administrowanie bazami"
    },
    "cert3": {
        "en": "Google Certificate - Basics of Internet Marketing.",
        "pl": " Certykiat Google - Podstawy marketinku internetowego"
    },
    "cert4": {
        "en": "Freeformers Certificate - Development and Digital Competence Training.",
        "pl": "Certyfikat Freeformers - Szkolenie rozwoju Kompetencji Cyfrowych"
    },
};

function switchLanguageFromURL() {
    var urlParams = new URLSearchParams(window.location.search);
    var lang = urlParams.get('lang');
    if (lang && translations[lang]) {
        switchLanguage(lang);
    } else {
        switchLanguage('pl');
        document.getElementById('englishButton').style.backgroundColor = 'rgb(231, 230, 230)';
    }
}

function switchLanguage(language) {
    var keys = Object.keys(translations);
    keys.forEach(function(key) {
        var element = document.getElementById(key);
        if (element) {
            element.textContent = translations[key][language];
        }
    });
}

function changeLanguage() {
    var actualLanguage = document.documentElement.lang;
    console.log(actualLanguage);

    if (actualLanguage === 'pl') {
        switchLanguage('en');
        document.documentElement.lang = 'en';
        document.getElementById('englishButton').style.backgroundColor = 'white';
        document.getElementById('polishButton').style.backgroundColor = 'rgb(231, 230, 230)';
    } else {
        switchLanguage('pl');
        document.documentElement.lang = 'pl';
        document.getElementById('englishButton').style.backgroundColor = 'rgb(231, 230, 230)';
        document.getElementById('polishButton').style.backgroundColor = 'white';
    }
}

switchLanguageFromURL();
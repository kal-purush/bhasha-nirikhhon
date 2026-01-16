const API_ADDRESS = "http://35.213.39.192/";

async function getPhonetics() {
    getData("phonetizer");
}

async function getKatakana() {
    getData("transcriber");
}

async function getData(resource) {
    const germanText = document.getElementById("germanText");
    const japaneseText = document.getElementById("japaneseText");
    japaneseText.value = '';
    try {
        const response = await fetch(API_ADDRESS + resource, {
            method: "POST",
            body: JSON.stringify({
                user_input: germanText.value
            }),
            headers: {
                "Content-type": "application/json; charset=UTF-8"
            }
        });
        if (!response.ok) {
            throw new Error(`Response status: ${response.status}`);
        }
        const json = await response.json();
        if (resource.localeCompare("transcriber") == 0) {
            for(let i = 0; i < json.length; i++) {
                japaneseText.value += json[i][3];
                if (json[i][1].localeCompare("PUNCT") != 0 && i != json.length - 1 && json[i+1][1].localeCompare("PUNCT") != 0) {
                    japaneseText.value += "・";
                }
            }
        }
    } catch (error) {
        console.error(error.message);
    }
}
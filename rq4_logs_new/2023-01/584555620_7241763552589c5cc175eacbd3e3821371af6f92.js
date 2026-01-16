var btnTranslate = document.querySelector("#btn-translate")
var txtInput = document.querySelector("#txt-input")
var outputDiv = document.querySelector("#output")

var serverURL = "https://api.funtranslations.com/translate/minion.json"

function getTranslationURL(text) {
    return serverURL + "?" + "text=" + text
}

function errorHandler(error) {
    alert("Something went wrong, try again later")
    console.log("error occured", error)
}


function clickHandler() {
    //taking input
    var inputTxt = txtInput.value

    //calling server 
    fetch(getTranslationURL(inputTxt))
        .then(response => response.json())
        .then(json => {
            outputDiv.innerHTML = json.contents.translated

        })
        .catch(errorHandler)

}

btnTranslate.addEventListener("click", clickHandler)
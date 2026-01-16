var pusht = document.querySelector("#push-t");
var text = document.querySelector("#ta");
var out = document.querySelector("#out");
var sURL = "https://api.funtranslations.com/translate/minion.json"

function transleationURL(input) {
    return sURL + "?" + "text=" + input
}

function errorH(error) {
    console.log("something wrong", error)
    alert("something wrong with sever")
}

function clickHandler() {
    var inputText = text.value;

    fetch(transleationURL(inputText))
        .then(response => response.json())
        .then(json => {
                var transText = json.contents.translated;
                out.innerText = transText;})
        .catch(errorH)
};
pusht.addEventListener("click", clickHandler)
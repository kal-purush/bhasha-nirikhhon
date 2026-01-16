function PromptUser() {
    let txt;
    let name = window.prompt("Enter your name");
    if(name !== null && name !== ""){
        txt = "Hello " + name;
        document.getElementById("name").innerHTML = txt
    } else {
        alert("Please enter your name.");
        PromptUser();
    }
}
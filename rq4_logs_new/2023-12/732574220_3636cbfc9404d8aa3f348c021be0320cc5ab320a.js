const input = document.querySelector(".chat");
const paragraph = document.querySelector(".paragraph");

function talk() {
    const user = input.value;
    
    var content = {
        "hello": "Hello, Who are u",
        "i am priya": "Good , What are you studying now a days ",
        "i am learning javascript" : "Ooo That's great",
        "yes thanku" : "your  welcome"
    };

    paragraph.innerHTML = user + "<br>";

    if (user in content) {
        paragraph.innerHTML = content[user] + "<br>";
    } else {
        paragraph.innerHTML = "Sorry, I can't understand";
    }

    input.value = ""
}
// call function
//loadDecks();

function greetUser(name) {
    var greetMess = document.getElementById("greeting");
//    console.log(greetMess);
    greetMess.innerHTML = greetMess.innerHTML + name;
}

function loadDecks(){
    var user, decks, deck;
    
    getUserInfo(function(returnVal){
        user = JSON.parse(returnVal);
        decks = user.myDecks;
//        console.log(user);
    },false);
    
    console.log(user);
    greetUser(user.username);
    
    var wrapper = $('#card_set'), container;

    for(var i = 0; i < decks.length; i++){
        getById(decks[i], function(e){
            deck = JSON.parse(e.substr(8));
//            console.log(deck);
        }, false);
        var newImage = document.createElement("div");
//        console.log(decks[i]);
        newImage.id = "div"+i;
        newImage.className = "images";
        newImage.innerHTML = 
            "<div class = \"image1\"> <center><h1>"+deck.title+"</h1><img src=\"images/CardSet.png\" width=\"304\" height=\"236\"></center>"
            +"\n <center>"
            +"\n <div class=\"buttonArea\">"
            +"\n <div class = \"study_button\">"
            +"\n <a id=\"studyBtn\" href=\"Study_Card.html?deck="+decks[i]+"\">"
            +"\n <button type=\"button\" class=\"btn btn-primary\">Study</button>"
            +"\n </a>"
            +"\n </div>"
            +"\n <div class = \"challenge_button\">"
            +"\n <a id=\"challengeBtn\" href=\"competition.html?deck="+decks[i]+"\">"
            +"\n <button type=\"button\" class=\"btn btn-danger\">Challenge</button>"
            +"\n </a>"
            +"\n </div>"
            +"\n <div class = \"edit_button\">"
            +"\n <a id=\"editBtn\" href=\"edit_a_set.html?deck="+decks[i]+"\">"
            +"\n <button type=\"button\" class=\"btn btn-danger\">Edit</button>"
            +"\n </a>"
            +"\n </div>"
            +"\n </div>"
            +"\n </center>"
            +"\n </div>";
        wrapper.append(newImage);
//                    wrapper.append(container);
//                    console.log(deck);
    }
}
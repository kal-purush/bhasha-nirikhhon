document.querySelector("button").addEventListener("click", function () {
    document.querySelector(".bar").classList.remove("active");

    var title = document.querySelector("h1");
    
    var player1 = document.querySelector(".img1");
    var player2 = document.querySelector(".img2");
    
    var player1Result = Math.floor(Math.random() * 6) + 1;
    var player2Result = Math.floor(Math.random() * 6) + 1;

    setTimeout(() => { 
        if (player1Result > player2Result) {
            title.innerHTML = "🚩 Player 1 Wins!";
        } else if (player1Result < player2Result) {
            title.innerHTML = "Player 2 Wins! 🚩";
        } else {
            title.innerHTML = "Draw!";
        }
        
        player1.setAttribute("src", "./images/dice" + player1Result + ".png");
        player2.setAttribute("src", "./images/dice" + player2Result + ".png");
    
        document.querySelector(".bar").classList.add("active");
    }, 1000);
    

});

document.querySelector("button").addEventListener("mouseover", function () {
    document.querySelector(".bar").style.backgroundColor = "rgb(156, 6, 12)";
})

document.querySelector("button").addEventListener("mouseout", function () {
    document.querySelector(".bar").style.backgroundColor = "#4ECCA3";
})
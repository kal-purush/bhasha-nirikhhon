const button = document.getElementById("button")

button.addEventListener("click",

    function () {

        const myNumber = Math.floor(Math.random() * 6) + 1;
        console.log(myNumber);
    }
);
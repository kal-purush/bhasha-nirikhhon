let main = document.querySelector(".centered");
let qGenBtn = document.querySelector("form");
//let refreshBtn = document.querySelector(".refresh");

function qGen(obj) {
    obj.preventDefault();
    fetch("https://opentdb.com/api.php?amount=10")
    .then(r => r.json())
    .then((d) => {
        let qList = d.results;
        for(let q of qList) {
            let qCard = document.createElement("article");
            let category = document.createElement("h2");
            let question = document.createElement("p");
            let toggleBtn = document.createElement("button");

            qCard.classList.add("card");

            if(q.difficulty === "easy") {
                qCard.classList.add("card1");
            } else if(q.difficulty === "medium") {
                qCard.classList.add("card2");
            } else if(q.difficulty === "hard") {
                qCard.classList.add("card3");
            }

            category.textContent = `${q.category} (${q.difficulty} level)`;
            question.textContent = q.question;
            toggleBtn.textContent = "Show Answer";

            main.append(qCard);
            qCard.append(category, question, toggleBtn);

            let answer = document.createElement("p");
            answer.classList.add("hidden");
            toggleBtn.addEventListener("click", (obj)=> {
                if(answer.classList.contains("hidden")) {
                    answer.classList.remove("hidden");
                    answer.textContent = q["correct_answer"];
                    qCard.append(answer);
                } else {
                    answer.classList.add("hidden");
                }
            });
        }
    })
    .catch(error => console.log(error));
}

qGenBtn.addEventListener("submit", qGen);

// refreshBtn.addEventListener("click", (e)=> {
//     location.reload();
// })
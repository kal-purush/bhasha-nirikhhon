document.addEventListener("DOMContentLoaded", () => {
    const btn = document.getElementById("btn");
    const result = document.getElementById("result");
    const smallcontainer = document.querySelector(".smallcontainer");

    const correctAnswers = {
        que1: "x = 5",
        que2: "58",
        que3: "function",
        que4: "let x = 5;"
    };

    btn.addEventListener("click", () => {
        let score = 0;

        for (let key in correctAnswers) {
            const selected = document.querySelector(`input[name="${key}"]:checked`);
            if (!selected) {
                alert("Answer all questions");
                return;
            }
            if (selected.value === correctAnswers[key]) score++;
        }

        smallcontainer.style.display = "block";
        result.textContent = `Score: ${score} out of 4`;
    });
});
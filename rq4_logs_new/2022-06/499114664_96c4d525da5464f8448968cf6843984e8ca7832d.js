const dice = document.querySelector(".dice");
const adviceId = document.querySelector(".adviceId");
const advice = document.querySelector(".advice");

dice.addEventListener("click", async function () {
    const res = await axios.get("https://api.adviceslip.com/advice");
    adviceId.innerHTML = res.data.slip.id;
    advice.innerHTML = res.data.slip.advice;
});
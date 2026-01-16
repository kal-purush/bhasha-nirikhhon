const app = document.querySelector("#app");
const button = document.createElement("button");
const buttonText = document.createTextNode("おみくじを引く");

let result = document.createTextNode("ここに結果表示");
button.addEventListener("click", () => {
  const n = Math.floor(Math.random() * 5);

  switch (n) {
    case 0:
      result.textContent = "凶";
      break;
    case 1:
      result.textContent = "末吉";
      break;
    case 2:
      result.textContent = "吉";
      break;
    case 3:
      result.textContent = "中吉";
      break;
    case 4:
      result.textContent = "末吉";
      break;
    default:
      result.textContent = "ここに結果表示";
  }
});

button.appendChild(buttonText);
app.appendChild(button);
app.appendChild(result);
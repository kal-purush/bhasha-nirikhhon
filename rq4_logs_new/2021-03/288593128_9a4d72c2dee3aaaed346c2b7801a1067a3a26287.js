function filteringErrorInput(input) {
  let field = null;
  if (input === "ingredients" || input === "preparation") {
    field = document.querySelectorAll(`input[name="${input}[]"]`);
  } else {
    field = document.querySelectorAll(`input[name="${input}"]`);
  }

  const upperDiv = field[0].parentElement;

  const cssRules = `
  <style>
    .item .error input{
      border-color: red;

      transition: 200ms;
    }
  </style>
  `;
  upperDiv.classList.add("error");

  upperDiv.insertAdjacentHTML("beforeend", cssRules);
  field[0].focus();
}

const input = document.querySelector(".inputBack-end");

//console.log(input.innerHTML);
filteringErrorInput(input.innerHTML);
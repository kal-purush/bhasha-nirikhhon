function addClass(e, classes) {
    e.classList && e.classList.add(...classes.split(" "));
  }
  
  function removeClass(e, classes) {
    e.classList && e.classList.remove(...classes.split(" "));
  }
  
  let data = {
    "complete-name": "",
    email: "",
    address: "",
    "phone-number": "",
    courier: "",
    payment: "",
  };
  
  const inputs = document.querySelectorAll("#shipping-detail input[data-input]");
  for (let index = 0; index < inputs.length; index++) {
    const input = inputs[index];
    input.addEventListener("change", function (e) {
      data[e.target.id] = e.target.value;
  
      check();
    });
  }
  
  const options = document.querySelectorAll("#shipping-detail button[data-name]");
  for (let index = 0; index < options.length; index++) {
    const option = options[index];
  
    option.addEventListener("click", function () {
      const value = this.attributes["data-value"].value;
      const name = this.attributes["data-name"].value;
  
      data[name] = value;
  
      check();
    });
  }
  
  function check() {
    const find = Object.values(data).filter((item) => item === "");
    if (find.length === 0) {
      document.querySelector(
        "#shipping-detail button[type='submit']"
      ).disabled = false;
    }
  }
  
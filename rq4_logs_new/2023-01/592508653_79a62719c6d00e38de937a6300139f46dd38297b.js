function ValidaCPF() {
  this.validateButton = document.querySelector(".validateButton");
  this.cleanButton = document.querySelector(".cleanButton");
  this.input = document.querySelector(".input");

  this.cleanButton.addEventListener("click", function () {
    this.input = document.querySelector(".input");
    this.input.className = "input";
    this.input.value = "";
  });

  this.validate = () => {
    this.validateButton.addEventListener("click", function () {
      this.input = document.querySelector(".input");
      this.cpf = this.input.value.replace(/\D/g, "");

      if (typeof this.cpf === "undefined") return false;
      if (this.cpf.length !== 11) return false;

      const partialCPF = this.cpf.slice(0, -2);

      const digit1 = createDigit(partialCPF, 10);
      const digit2 = createDigit(partialCPF + digit1, 11);

      const validatedCpf = partialCPF + digit1 + digit2;

      if (this.cpf === validatedCpf) {
        this.input.classList.remove("invalid");
        this.input.classList.add("valid");
        alert("CPF válido!");
      } else {
        this.input.classList.remove("valid");
        this.input.classList.add("invalid");
        alert("CPF inválido!");
      }
    });
  };

  const createDigit = (partialCPF, cont) => {
    let i = cont;
    let soma = 0;

    while (i >= 2) {
      for (let el of partialCPF) {
        soma += el * i;
        i--;
      }
    }
    const digit = 11 - (soma % 11);
    return digit > 9 ? "0" : String(digit);
  };
}

const cpf = new ValidaCPF();
cpf.validate();
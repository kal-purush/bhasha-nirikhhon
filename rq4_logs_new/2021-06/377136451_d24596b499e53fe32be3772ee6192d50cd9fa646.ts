const inbox = document.getElementById("enter-box");
const outbox = document.getElementById("out-box");

function crazyCase(input: string) {
  let out = "";
  for (let i = 0; i < input.length; i++) {
    let factor = 50000000; // randomness factor
    let rando = parseInt((Math.random() * factor).toFixed(0)); // get random number
    let divider = parseInt((Math.random() * 10).toFixed(0));
    if (rando % divider === 0) {
      // if even uppercase
      out += input[i].toUpperCase();
    } else {
      // else lowercase even the Caps
      out += input[i].toLowerCase();
    }
  }
  return out; // return randomly cased string
}

function onChange(e: Event) {
  outbox.innerHTML = crazyCase(this.value); // add to outbox
}

inbox.addEventListener("input", onChange); //
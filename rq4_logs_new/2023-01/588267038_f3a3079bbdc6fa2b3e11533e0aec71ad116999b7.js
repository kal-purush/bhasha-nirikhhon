// Get elements
const letterKeys = [...document.getElementsByClassName("key")]; // Letter keys only
const numKeys = [...document.getElementsByClassName("num-key")]; // Number keys
const keyboards = [...document.getElementsByClassName("keyboard")]; // Both keyboards
const paper = document.getElementById("paper"); // Text area
const capsKeys = [...document.getElementsByClassName("caps-lock")]; // Shift key
const specialKeys = [...document.getElementsByClassName("special")]; // Special key
const newLineKeys = [...document.getElementsByClassName("new-line")]; // Enter key
const spaceKeys = [...document.getElementsByClassName("space")]; // Space key
const deleteKeys = [...document.getElementsByClassName("delete")]; // Delete key

// Textarea array
const chars = [];

// Add typing function to letter keys
letterKeys.forEach((key) =>
  key.addEventListener("click", () => {
    paper.value += key.innerText;
    chars.push(key.innerText);
    console.log(chars);
  })
);

// Add typing function to number keys
numKeys.forEach((key) =>
  key.addEventListener("click", () => {
    paper.value += key.innerText;
    chars.push(key.innerText);
  })
);

// Setup delete key
deleteKeys.forEach((deleteKey) =>
  deleteKey.addEventListener("click", () => {
    console.log("DELETE");
    chars.pop();
    paper.value = chars.join("");
    console.log(chars);
  })
);

// Setup space key
spaceKeys.forEach((spaceKey) =>
  spaceKey.addEventListener("click", () => {
    paper.value += " ";
    chars.push(" ");
  })
);

// Setup capslock key
capsKeys.forEach((capsKey) =>
  capsKey.addEventListener("click", () => {
    letterKeys.forEach((key) => key.classList.toggle("uppercase"));
  })
);

// Setup Enter key
newLineKeys.forEach((newLineKey) =>
  newLineKey.addEventListener("click", () => {
    paper.value += "\n";
    chars.push("\n");
  })
);

// Setup special key
specialKeys.forEach((specialKey) =>
  specialKey.addEventListener("click", () => {
    keyboards.forEach((keyboard) =>
      keyboard.classList.toggle("hidden-keyboard")
    );
  })
);
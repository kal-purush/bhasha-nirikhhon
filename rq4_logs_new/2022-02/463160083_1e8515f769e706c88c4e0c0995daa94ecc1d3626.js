const boxes = document.querySelectorAll(".box");
const input = document.getElementById("input");


// console.log(boxes);
// let oddColorizer = (color) => {
//   boxes.forEach((e, key) => {
//     if (key) e.style = ` background: ${color}; `;
//     else e.style = ` background: ${color};  `;
//   });
// };



let oddColorizer = (block) => {
  const arr = block.split(" ");
  console.log(arr);
  boxes.forEach((e, key) => {
    e.style = `
    background: ${arr[0]};
    border-radius: ${arr[1]};
    height: ${arr[2]}
    width: ${arr[3]};
    
    `;
  });
};

window.addEventListener("keyup", (e) => {
  if (e.key == "Enter") {
    oddColorizer(input.value);
    input.value = "";
  }
});
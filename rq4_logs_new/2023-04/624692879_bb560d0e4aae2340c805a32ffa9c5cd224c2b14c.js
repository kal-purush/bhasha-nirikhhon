


let increase = document.getElementById("count-el");
let record = document.getElementById("save-el");
let count = 0;

function increment() {
   count++
   increase.textContent = count


c
}

function save() {
    para = count + " - "

    record.textContent += para;
    count = 0;
    increase.textContent = count


}
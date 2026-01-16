let exchan1=document.querySelectorAll('.container3 .selection label');
let exchan2=document.querySelectorAll('.container4 .selection label');
let inputTo=document.getElementById('number_to');
let inputFrom=document.getElementById('number_from');
let ParagraphFrom = document.querySelector(".paragraph_from");
let ParagraphTo = document.querySelector(".paragraph_to");
var currency1=document.getElementById('EUR1').value;
var currency2=document.getElementById('USD2').value;
eventListeners();
function eventListeners() {
  inputFrom.addEventListener("keyup", checkDataByFrom);
  inputTo.addEventListener("keyup", checkDataByTo);
}

exchan1.forEach((select) => {
    select.addEventListener('click',(e)=>{
        currency1=event.target.innerText
        console.log(currency1)
        checkDataByTo()
    })
})
exchan2.forEach((select) => {
    select.addEventListener('click',(e)=>{
        currency2=event.target.innerText
        console.log(currency2)
        checkDataByFrom()
    })
})

async function checkDataByFrom() {
  const res = await fetch(`https://api.exchangerate.host/latest?base=${currency1}&symbols=${currency2}`);
  const data = await res.json();
  inputTo.value =( Object.values(data.rates)[0] * inputFrom.value).toFixed(2);
  if (currency1&&currency2){
 ParagraphFrom.innerHTML = `1 ${data.base} = ${Object.values(data.rates)[0].toFixed(5)} ${Object.keys(data.rates)}`;
 ParagraphTo.innerHTML= `1 ${Object.keys(data.rates)} = ${(1/Object.values(data.rates)[0]).toFixed(5)} ${data.base}`;
}
}
async function checkDataByTo() {
  const res = await fetch(`https://api.exchangerate.host/latest?base=${currency1}&symbols=${currency2}`);
  const data = await res.json();
  inputFrom.value = (inputTo.value / Object.values(data.rates)[0]).toFixed(4);
}
const btnEl = document.getElementById('calculate');
const billEl  = document.getElementById('bill');
const tipEl = document.getElementById('tip');
const totalEL  = document.getElementById('total');
const calTotal = () => {
    let billValue = billEl.value ;
    let tipValue = tipEl.value ;
    let totalAmount  = billValue * (1 +  tipValue / 100);
    totalEL.innerText = ` Total: $${totalAmount.toFixed(2)}`;
}
    

btnEl.addEventListener('click', calTotal)
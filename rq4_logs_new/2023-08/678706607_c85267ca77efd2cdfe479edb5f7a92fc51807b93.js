function findAmount(event)
{
    event.preventDefault();
let billAmt=parseFloat(document.getElementById('billAmount').value);
let tipAmt=parseFloat(document.getElementById('tipAmount').value);
if(validate(billAmt,tipAmt))
{
let totalBill=billAmt+((billAmt*tipAmt)/100);
let total=document.getElementById('total');
total.textContent='Total: '+'\u20B9 '+`${totalBill.toFixed(2)}`;
document.getElementsByTagName('input')[0].value='';
document.getElementsByTagName('input')[1].value='';
}
else{
alert('Please enter valid input')
document.body.style.op
}
}

function validate(billAmt,tipAmt)
{
    return !isNaN(billAmt) && !isNaN(tipAmt) && billAmt >= 0 && tipAmt >= 0;
}
document.getElementById('Calculate-btn').addEventListener('click', function(){
    const myIncome = document.getElementById('iname').value;
    const totalExpensesElement = innerText('totalExpenses');
    totalExpensesElement.innerText =  bueProduct();
    const blanceElement = document.getElementById('blance');
    blanceElement.innerText = parseInt(myIncome) -  bueProduct();
    

})

function bueProduct(){
    const foodElement = totalCost('fname');
    const rentElement = totalCost('rname');
    const clotherElement = totalCost('cname');
    const total = parseInt(foodElement) + parseInt(rentElement) + parseInt(clotherElement);
    return total;
}

function innerText(id){
    const text = document.getElementById(id);
    return text;
}

function totalCost(id){
    const foodElement = document.getElementById(id).value;
    return foodElement;
}



document.getElementById('saveingbtn').addEventListener('click', function(){
    const myBlance = totalCost('iname');
    const saveingField= totalCost('sname');

    const saveingAmountElement = document.getElementById('savingbtn');

    // const remaningBtnElement = document.getElementById('remaningbtn');
    

    saveingAmountElement.innerText = parseInt(myBlance) * parseInt(saveingField) / 100;
   
    // remaningBtnElement.innerText = remaning;


    
    
})

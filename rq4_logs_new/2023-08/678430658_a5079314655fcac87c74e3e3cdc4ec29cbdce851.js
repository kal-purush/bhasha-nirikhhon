document.getElementById('dep-btn').addEventListener('click', function () {
    const depos = document.getElementById('depo');
    const depositString = depos.value;
    const deposit = parseFloat(depositString)


    const deposAmount = document.getElementById('depo-tk');
    const depositeAmountString = deposAmount.innerText
    const depositeAmount = parseFloat(depositeAmountString)

    const totalDeposite = depositeAmount + deposit;
    deposAmount.innerText = totalDeposite;

    const balance = document.getElementById('bal');
    const balanceString = balance.innerText;
    const balanceTotal = parseFloat(balanceString);

    const newBalance = deposit + balanceTotal;
    balance.innerText = newBalance;

    depos.value = ''


})


document.getElementById('wid-btn').addEventListener('click', function () {
    const withdraw = document.getElementById('widh');
    const widhValue = withdraw.value;
    const totalWidhdraw = parseFloat(widhValue);

    withdraw.value = '';

    if(isNaN(totalWidhdraw)){
        return alert('Please Provide Number')
    }

    const widhBalance = document.getElementById('widh-bal');
    const widhString = widhBalance.innerText;
    const totalWidhBalance = parseFloat(widhString);

    

    const balance = document.getElementById('bal');
    const balanceString = balance.innerText;
    const totalBalance = parseFloat(balanceString);

   

    if(totalWidhdraw>totalBalance){
        return alert('You Cant Withdraw More Than Balance')
    }
    

    const value = totalBalance-totalWidhdraw;
    balance.innerText=value;

    
    const totalWidhdrawBlanance = totalWidhdraw + totalWidhBalance;
    widhBalance.innerText = totalWidhdrawBlanance;

    


})
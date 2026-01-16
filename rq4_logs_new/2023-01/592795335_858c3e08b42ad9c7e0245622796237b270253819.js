const inputDeposite = document.querySelector('.deposite-input');
const withdrawInput = document.querySelector('.withdraw-input');
const depositeBtn = document.querySelector('.depoBtn');
const withdrawBtn = document.querySelector('.withdrawBtn');


var finalBalance;

depositeBtn.addEventListener('click', function () {

    function validForm() {
        if (inputDeposite.value == "" || inputDeposite.value == null) {
            alert('Number must be filledout');
            return false;
        } else {
            let inputNumber = inputDeposite.value;
            let inputDepoNumber = parseFloat(inputNumber);
        
            const displayDepo = document.querySelector('.display-deposite').innerText;
            let currentDepo = parseFloat(displayDepo);
        
            const total = currentDepo + inputDepoNumber;
            document.querySelector('.display-deposite').innerText = total;

                //total balance//
    
            const totalBalance = document.querySelector('.total-bal').innerText;
            let totalBalNumber = parseFloat(totalBalance);//total balance number
            finalBalance = inputDepoNumber + totalBalNumber;
            document.querySelector('.total-bal').innerText = finalBalance;
            inputDeposite.value = '';
                //end total balance//
        }
    }

    validForm();

})

//withdraw event handler //

withdrawBtn.addEventListener('click', function () {

    function validForm2() {
        if (withdrawInput.value == "" || withdrawInput.value == null) {
            alert('Number must be filledout');
            return false;
        } else {
            let inputWithdrawAmount = withdrawInput.value;
            let withDrawValue = parseFloat(inputWithdrawAmount);
            
            const displayWithdrawAmount = document.querySelector('.display-withdraw').innerText;
            let withDrawNumber = parseFloat(displayWithdrawAmount);
            const totalWithDraw = withDrawNumber + withDrawValue;
            document.querySelector('.display-withdraw').innerText = totalWithDraw;
            let totalBalanceAmount = document.querySelector('.total-bal').innerText;
            let totalBalanceNumber = parseFloat(totalBalanceAmount);
            if (totalBalanceNumber <= 0) {
                alert('no balance');
                return false;
            } else {
                let minusWithdraw = totalBalanceNumber - withDrawValue;
                document.querySelector('.total-bal').innerText = minusWithdraw;
                withdrawInput.value = '';  
            }

        }
    }
    validForm2();
})



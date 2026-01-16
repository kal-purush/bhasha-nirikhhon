let billAmount = document.querySelector('#bill-amt')

let tipAmount = document.querySelector('#tip-amt')

let button = document.querySelector('#btn')

let display = document.querySelector('#display')

function calculate(){

   
  
        let billamount = parseFloat(billAmount.value);
        let tipamount = parseFloat(tipAmount.value);

        if(tipamount>100){
            alert("Tip Amount Cannot Exceed 100")
            tipAmount.value=""
        }else{
            let total = billamount+tipamount;
            console.log(total)
        
            result.innerText = total

            // billAmount.value = "";
            // tipAmount.value =""

            setTimeout(function(){
            billAmount.value = "";
            tipAmount.value =""
            },2000)
        
        }
    
       
    


}



button.addEventListener('click',calculate)
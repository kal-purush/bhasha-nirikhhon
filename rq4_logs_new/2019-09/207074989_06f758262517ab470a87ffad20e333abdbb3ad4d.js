const josephusInput = document.getElementById('josephus-input');
const josephusBtn = document.getElementById('josephus-btn');

josephusBtn.addEventListener('click', function(){
    let numOfSoldiers = josephusInput.value;
    if (isNaN(numOfSoldiers)){
        alert('Please Enter a Number');
        return;        
    }

    var binaryNumOfSoldiers = parseInt(numOfSoldiers).toString(2);
    var binaryResult = binaryNumOfSoldiers.substr(1) + binaryNumOfSoldiers[0]; 
    
    alert(`Josephus Position: ${parseInt(binaryResult, 2)}`);
});
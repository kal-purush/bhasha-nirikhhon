export const checkCreditCardNumber = (num) => {
    if(num.length != 16) {
        return false  
    }
    let i 
    let singleNum = []
    let doubleNum = []
    let numSplit = num.split("") 
    for (i = 15; i >= 0; i--) {
        if((i+1) % 2 == 0) {
            singleNum.push(Number(numSplit[i]))
        }
        else {
            let number = Number(numSplit[i])
            if (number*2 > 9) {
                let text = String(number*2)
                let numArr = text.split("")
                let sum = 0 
                numArr.map(n => {
                    sum += Number(n) 
                })
                doubleNum.push(sum) 
            }
            else doubleNum.push(number*2)
        }
    }

    
    let oddSum = singleNum.reduce((partialSum, a) => partialSum + a, 0);
    let evenSum = doubleNum.reduce((partialSum, a) => partialSum + a, 0);

    if ((oddSum + evenSum) % 10 == 0) { 
        return true 
    }
    return false 
}
//1
function findPrime(...numbers) {
    for (let i = 0; i < numbers.length; i++) {
        if (numbers[i] < 0) {
            continue
        }
        let asalMi = true;
        for (let j = 2; j < numbers[i]; j++) {
            if (numbers[i] % j == 0) {
                asalMi = false;
            }
        }
        if (asalMi) {
            console.log(`asal ${numbers[i]}`)
        } else {
            console.log(`asal değil ${numbers[i]}`)
        }
    }
}
findPrime(-123, 365, 5, 3, 24, 21, 34, 6, 4, 2);
//************************************************
console.log("************************************************")
//2
function arkadasMi(number1, number2) {
    let num1Total = 0,
        num2Total = 0;
    //num1
    for (let i = 0; i < number1; i++) {
        if (number1 % i == 0) {
            num1Total += i;
        }
    }
    //num2
    for (let i = 0; i < number2; i++) {
        if (number2 % i == 0) {
            num2Total += i;
        }
    }
    console.log((num1Total == number2) && (num2Total == number1) ?
        `${number1} ve ${number2} arkadaş sayılar.` :
        `${number1} ve ${number2} arkadaş sayı değil.`);
}
arkadasMi(220, 284)
//************************************************
console.log("************************************************")
//3
function mukemmelSayilar() {
    for (let i = 1; i <= 1000; i++) {
        let total = 0;
        for (let j = 1; j < i; j++) {
            if (i % j == 0) {
                total += j;
            }
        }
        if (i == total) {
            console.log(i);
        }
    }
}
mukemmelSayilar()
//************************************************
console.log("************************************************")
//4
function asalSayilar() {
    for (let i = 1; i <= 1000; i++) {
        let asalmi = true;
        for (let j = 2; j < i; j++) {
            if (i % j == 0) {
                asalmi = false;
                break;
            }
        }
        if (asalmi) console.log(i)
    }
}
asalSayilar()
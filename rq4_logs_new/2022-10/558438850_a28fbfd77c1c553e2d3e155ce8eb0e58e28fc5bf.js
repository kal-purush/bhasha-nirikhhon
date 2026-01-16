
const stringFunc = (numString) =>{
const result =translateFunction(numString);
const funcReplaceValues = replaceValue(stringToArr(numString));

let res;
return CheckIfStringContainOnlyNumbers(result)
? (res = funcReplaceValues.join(""))
: (res = result + removeAllDigits(funcReplaceValues.join("")));

};

const translateFunction =(num)=>{
let res = "";
if(num % 3 === 0) res += "Foo";
if(num % 5 === 0) res += "Bar";
if(num % 7 === 0) res += "Qix";
//si le number n'est pas devisble on retourne le number 
if(res === "") res += num
return res;
};

//const stringToArr = (str) => str.split("");
function stringToArr(str){
   const res = str.split("")
return res
}


//vérifier si les valeur dans le tableau contiennent des conditions de valeurs
const values =  [3,5,7,0];
const replace = ["Foo", "Bar","Qix","*"]
const replaceValue = (numStringArr) =>{
    const arrValue = numStringArr.map((e) => { 
        console.log(numStringArr)
        let containValue = "";
        if(e === values[0].toString()) containValue += replace[0];
        if(e === values[1].toString()) containValue += replace[1];
        if(e === values[2].toString()) containValue += replace[2]
        if(e === values[3].toString()) containValue += replace[3]
        //retourner inial values
        if(containValue === "")containValue += e;
        return containValue;
    })
return arrValue
};


//vérifier si string contient uniquement nes nombres

const CheckIfStringContainOnlyNumbers =(str)=>{
    return /^[0-9]+$/.test(str)
};

//retirer les nombres et laisser que les strings
const removeAllDigits = (arr)=>{
    return arr.replace(/[0-9]/g, "");
}

console.log(CheckIfStringContainOnlyNumbers("521b2"))
console.log(CheckIfStringContainOnlyNumbers("123"))
console.log(translateFunction("105"))



module.exports = {
    translateFunction,
    replaceValue,
    CheckIfStringContainOnlyNumbers,
    removeAllDigits,
    stringFunc

} 
// function nameValidation(firstName, lastName){
//     if (firstName == "" || lastName == "") {
//         console.log('Nama Kosong')
//     } else {
//         console.log('Success')
//     }
// }

// nameValidation("asdfa", "sdfa")

// const inputField = document.getElementById("tinggi-input");
// const button = document.getElementById('count-button')
// inputField.addEventListener('input', function(){
//     const inputValue = inputField.value
    
//     if (inputValidation(inputValue)){
//         console.log('Success')
//         // button.style.display = "block"
//         button.disabled = false
//     } else{
//         console.log('Invalid')
//         button.disabled = true
//         // button.style.display = "none"
//     }
// }) 

// function inputValidation(input){
//     const integerRegex = /^-?\d+$/
//     return integerRegex.test(input)
// }

try {
    var bilangan = 10
    var pembagi = 0

    if (pembagi == 0) {
        throw new Error('Pembagi bernilai 0')
    }
    console.log("Succsess")
} catch (error) {
    console.log("Ada error", error)
    alert("Maaf ada yang error")
}
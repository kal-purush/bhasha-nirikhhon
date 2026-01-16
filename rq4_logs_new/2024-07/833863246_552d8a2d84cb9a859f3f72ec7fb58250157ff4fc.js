let input = document.getElementById("input-num")
let convert = document.getElementById("convert-btn")
let length = document.getElementById("length")
let volume = document.getElementById("volume")
let mass = document.getElementById("mass")

length.textContent = "0 meters = 0.00 feet | 0 feet = 0.00 meters"
volume.textContent = "0 liters = 0.00 gallons | 0 gallons = 0.00 liters"
mass.textContent = "0 kilos = 0.00 pounds | 0 pounds = 0.00 kilos"

convert.addEventListener("click", function () {
    unitConverter()
    input.value = ""
})

input.addEventListener("keypress", function () {
    if (event.key === "Enter") {
        unitConverter();
        input.value = ""
    }
})

function unitConverter() {
    let inputValues = input.value
    length.textContent = `${inputValues} meters = ${(inputValues * 3.281).toFixed(2)} feet | ${inputValues} feet = ${(inputValues / 3.281).toFixed(2)} meters`
    volume.textContent = `${inputValues} liters = ${(inputValues * 0.264).toFixed(2)} gallons | ${inputValues} gallons = ${(inputValues / 0.264).toFixed(2)} liters`
    mass.textContent = `${inputValues} kilos = ${(inputValues * 2.204).toFixed(2)} pounds | ${inputValues} pounds = ${(inputValues / 2.204).toFixed(2)} kilos`
}
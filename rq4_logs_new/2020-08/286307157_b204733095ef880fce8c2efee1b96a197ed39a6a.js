//Procurar o botão
document.querySelector("#add-time")
//Quando clicar no botão
.addEventListener('click', cloneField)
//Executar uma ação
function cloneField() {
    const newfieldContainer = document.querySelector('.schedule-item').cloneNode(true)
    const fields = newfieldContainer.querySelectorAll('input')
    fields.forEach(function(field){
        field.value = ""
    })
    document.querySelector('#schedule-items').appendChild(newfieldContainer)
}
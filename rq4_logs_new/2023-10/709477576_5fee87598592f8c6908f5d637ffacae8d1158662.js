const form = document.getElementById("novoItem")

form.addEventListener("submit",(evento) => {
    evento.presenteDefault()

    console.log(evento.targetelements('nome').value)
    console.log(evento.targetelements('quantidade').value)
})
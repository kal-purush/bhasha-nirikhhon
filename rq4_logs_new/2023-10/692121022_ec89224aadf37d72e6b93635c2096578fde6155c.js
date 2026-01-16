//Al cancelar el cambio, se llama a la función exit que dispara una alerta y devuelve a la página index.html
function exit() {
    document.getElementById("exitAlert").classList.remove("collapse");
    setTimeout( ()=> {
        document.getElementById("exitAlert").classList.add("collapse");
        window.location.href = "index.html"
    }, "2500");
};

//Al guardar el cambio, se dispara la función save que toma los nuevos inputs y los envía con el método PUT,
// dispara una alerta de envío y devuelve a la página index.html
function save() {
    let name = document.getElementById("name").value;
    let surname = document.getElementById("surname").value;
    let group = document.getElementById("group").value;
    let room = document.getElementById("room").value;

    if (name === "" || surname === "" || group === "" || room === "" || name === null || surname === null || group === null || room === null) {
        document.getElementById("fillAlert").classList.remove("collapse");
        setTimeout(()=>{
            document.getElementById("fillAlert").classList.add("collapse");        
        }, "2000");
        console.log("Alerta Formulario sin completar");
    } else {
        fetch(`https://crudcrud.com/api/adc54d7744e946cd8ffc1851accabb6d/grupo264/${localStorage.getItem("user")}`, {
            headers: { "Content-Type": "application/json; charset=utf-8" },
            method: 'PUT',
            body: JSON.stringify({
                nombre: name,
                apellido: surname,
                grupo: group,
                sala: room
            })
        })
        .then(response => console.log("Datos guardados correctamente"))
        .catch((error) => console.error("Error:", error));
        document.getElementById("editAlert").classList.remove("collapse");
        setTimeout(()=>{
            document.getElementById("editAlert").classList.add("collapse");
            window.location.href = "index.html";
        }, "2500");
    };
};

//Al cargar la página se ejecuta la función getPrevData, que trae la infromación del recurso guardado en el localStorage,
//y la carga en el formulario para recordar al usuario el tipo de datos previamente cargado.
async function getPrevData() {
    fetch(`https://crudcrud.com/api/adc54d7744e946cd8ffc1851accabb6d/grupo264/${localStorage.getItem("user")}`) 
    .then(response => response.json())
    .then(data => {
        document.getElementById("name").value = `${data.nombre}`;
        document.getElementById("surname").value = `${data.apellido}`;
        document.getElementById("group").value = `${data.grupo}`;
        document.getElementById("room").value = `${data.sala}`;
        console.log("Datos previos cargados");
    })
    .catch((error) => console.log("Error: ", error));
}

document.addEventListener("DOMContentLoaded", ()=> {
    getPrevData();
    document.getElementById("exit").addEventListener("click", exit);
    document.getElementById("save").addEventListener("click", save);
});
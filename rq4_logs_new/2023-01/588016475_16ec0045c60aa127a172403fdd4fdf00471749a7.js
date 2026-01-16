const firebaseConfig = {
    apiKey: "AIzaSyDl4xRPfLJXRJyzGkOVUc2idhz9fBf0BO8",
    authDomain: "asistencia-bfae3.firebaseapp.com",
    projectId: "asistencia-bfae3",
    storageBucket: "asistencia-bfae3.appspot.com",
    messagingSenderId: "49143224673",
    appId: "1:49143224673:web:072e982fac0ba4417e4d20",
    measurementId: "G-7X0F1C16T8",
    databaseURL: "https://asistencia-bfae3-default-rtdb.firebaseio.com/",
};

const openModal = document.getElementById('openRegisterModal');
const modal = document.getElementById('modal');
const modalUpdate = document.getElementById('modal-update');
const updateForm = document.getElementById('update-form')
const closeUpdateModal = document.getElementById('closeUpdateModal')
const closeModal = document.getElementById('closeRegisterModal')
const registerForm = document.getElementById('register-form')
const asistantTable = document.getElementById('asistantTable')

firebase.initializeApp(firebaseConfig)
const asistantRef = firebase.database().ref('asistencia')

const showRegisterModal = () => {
    modal.classList.toggle('is-active')
}

openModal.addEventListener('click', showRegisterModal)
closeModal.addEventListener('click', showRegisterModal)

const deleteAsistente = (uid) => {
    firebase.database().ref(`asistencia/${uid}`).remove()
}

const showUpdateModal = () => {
    modalUpdate.classList.toggle('is-active')
}

closeUpdateModal.addEventListener('click', showUpdateModal)

/* Código para actualizar inputs automáticamente */
const inputFields = [registerForm['caballeros'], registerForm['dorcas'], registerForm['amigos'], registerForm['simpatizantes'], registerForm['asistentes'], registerForm['ninos']];
const totalInput = registerForm['total'];

// add event listener to each input field
inputFields.forEach(inputField => {
    inputField.addEventListener('input', updateTotal);
});

// update total function
function updateTotal() {
    let total = 0;
    inputFields.forEach(inputField => {
        total += Number(inputField.value);
    });
    totalInput.value = total;
}
/* Fin del código */

window.addEventListener('DOMContentLoaded', async (e) => {
    await asistantRef.on('value', (asistentes) => {
        asistantTable.innerHTML = ''
        let counter = 1
        asistentes.forEach((asistente) => {
            let asistenteData = asistente.val()
            asistantTable.innerHTML += `<tr>
    <th>${counter}</th>
    <td>${asistenteData.Fecha}</td>
    <td>${asistenteData.Servicio}</td>
    <td>${asistenteData.Caballeros}</td>
    <td>${asistenteData.Dorcas}</td>
    <td>${asistenteData.Amigos}</td>
    <td>${asistenteData.Simpatizantes}</td>
    <td>${asistenteData.Asistentes}</td>
    <td>${asistenteData.Ninos}</td>
    <td>${asistenteData.Ofrendas}</td>
    <td>${asistenteData.Descripcion}</td>
    <td>${asistenteData.Total}</td>
    <td>
<button class="button is-warning" data-id="${asistenteData.Uid}">
<i class="fa-solid fa-pencil"></i>
</button>
<button class="button is-danger" data-id="${asistenteData.Uid}">
<i class="fa-solid fa-trash-can"></i>
</button>
</td>
    </tr>`
            counter++
            const updateButtons = document.querySelectorAll('.is-warning')
            updateButtons.forEach((button) => {
                button.addEventListener('click', (e) => {
                    showUpdateModal()
                    firebase.database().ref(`asistencia/${e.composedPath()[1].dataset.id}`).once('value').then((asistente) => {
                        const data = asistente.val()
                        updateForm['fecha'].value = data.Fecha
                        updateForm['servicios'].value = data.Servicio
                        updateForm['caballeros'].value = data.Caballeros
                        updateForm['dorcas'].value = data.Dorcas
                        updateForm['amigos'].value = data.Amigos
                        updateForm['simpatizantes'].value = data.Simpatizantes
                        updateForm['asistentes'].value = data.Asistentes
                        updateForm['ninos'].value = data.Ninos
                        updateForm['ofrendas'].value = data.Ofrendas
                        updateForm['mensaje'].value = data.Descripcion
                        updateForm['total'].value = data.Total
                    })
                    const uid = e.composedPath()[1].dataset.id
                    updateForm.addEventListener('submit', (e) => {
                        e.preventDefault()

                        const fecha = updateForm['fecha'].value
                        const servicio = updateForm['servicios'].value
                        const caballeros = updateForm['caballeros'].value
                        const dorcas = updateForm['dorcas'].value
                        const amigos = updateForm['amigos'].value
                        const simpatizantes = updateForm['simpatizantes'].value
                        const asistentes = updateForm['asistentes'].value
                        const ninos = updateForm['ninos'].value
                        const ofrendas = updateForm['ofrendas'].value
                        const descripcion = updateForm['mensaje'].value
                        const total = updateForm['total'].value

                        firebase.database().ref(`asistencia/${uid}`).update({
                            Fecha: fecha,
                            Servicio: servicio,
                            Caballeros: caballeros,
                            Dorcas: dorcas,
                            Amigos: amigos,
                            Simpatizantes: simpatizantes,
                            Asistentes: asistentes,
                            Ninos: ninos,
                            Ofrendas: ofrendas,
                            Descripcion: descripcion,
                            Total: total
                        })
                        showUpdateModal()
                    })
                })
            })

            const deleteButtons = document.querySelectorAll('.is-danger')
            deleteButtons.forEach((button) => {
                button.addEventListener('click', (e) => {
                    deleteAsistente(e.composedPath()[1].dataset.id)
                })
            })
        })
    })
})

registerForm.addEventListener('submit', (e) => {
    e.preventDefault()
    const fecha = registerForm['fecha'].value
    const servicio = registerForm['servicios'].value
    const caballeros = registerForm['caballeros'].value
    const dorcas = registerForm['dorcas'].value
    const amigos = registerForm['amigos'].value
    const simpatizantes = registerForm['simpatizantes'].value
    const asistentes = registerForm['asistentes'].value
    const ninos = registerForm['ninos'].value
    const ofrendas = registerForm['ofrendas'].value
    const descripcion = registerForm['mensaje'].value
    const total = registerForm['total'].value

    // Check for empty or invalid inputs
    if (!fecha || !servicio || !caballeros || !dorcas || !amigos || !simpatizantes || !asistentes || !ninos || !total) {
        alert("Campos Requeridos para valores ingrese 0");
        return;
    }
    // Check for negative values
    if (caballeros < 0 || dorcas < 0 || amigos < 0 || simpatizantes < 0 || asistentes < 0 || ninos < 0 || ofrendas < 0) {
        alert("Los valores no pueden ser negativos!");
        return;
    }
    if (total != (Number(caballeros) + Number(dorcas) + Number(amigos) + Number(simpatizantes) + Number(asistentes) + Number(ninos))) {
        alert("Los valores de los campos no coinciden con el total");
        return;
    }


    const registerAsistencia = asistantRef.push()

    registerAsistencia.set({
        Uid: registerAsistencia.path.pieces_[1],
        Fecha: fecha,
        Servicio: servicio,
        Caballeros: caballeros,
        Dorcas: dorcas,
        Amigos: amigos,
        Simpatizantes: simpatizantes,
        Asistentes: asistentes,
        Ninos: ninos,
        Ofrendas: ofrendas,
        Descripcion: descripcion,
        Total: total
    })
    showRegisterModal()
})
var calendar;
document.addEventListener('DOMContentLoaded', function () {
    var calendarEl = document.getElementById('calendar');
    var modal = document.getElementById('eventModal');
    var modalTitle = document.getElementById('modalTitle');
    var eventTitleInput = document.getElementById('eventTitleInput');
    var saveBtn = document.getElementById('saveBtn');
    var deleteBtn = document.getElementById('deleteBtn');
    var cancelBtn = document.getElementById('cancelBtn');
    var selectedEvent = null; // Guardar el evento seleccionado para editar/eliminar
    var dateSelect = '';

    // Inicializar el calendario
    calendar = new FullCalendar.Calendar(calendarEl, {
        locale: 'es',
        initialView: 'dayGridMonth',
        editable: true,
        events: function (fetchInfo, successCallback, failureCallback) {
            listarRecordatorios(successCallback, failureCallback);
        },
        // Crear un nuevo recordatorio al hacer clic en una fecha
        dateClick: function (info) {
            dateSelect = info.dateStr;
            selectedEvent = null; // Limpiar el evento seleccionado
            eventTitleInput.value = ''; // Limpiar el campo de título
            modalTitle.textContent = 'Crear nuevo recordatorio';
            deleteBtn.style.display = 'none'; // Ocultar botón de eliminar al crear
            modal.style.display = 'block'; // Mostrar el modal
        },
        // Editar o eliminar al hacer clic en un evento
        eventClick: function (info) {
            selectedEvent = info.event; // Guardar el evento seleccionado
            eventTitleInput.value = info.event.title; // Mostrar el título actual en el input
            modalTitle.textContent = 'Editar o eliminar recordatorio';
            deleteBtn.style.display = 'inline-block'; // Mostrar el botón de eliminar
            modal.style.display = 'block'; // Mostrar el modal
        },
        eventDrop: function (info) {
            let newDate = info.event.startStr;
            modificarFechaRecordatorio(info.event.id, newDate);
        }
    });

    calendar.render();


    // Botón para guardar (crear o modificar)
    saveBtn.addEventListener('click', function () {
        let title = eventTitleInput.value;
        if (title) {
            if (selectedEvent) {
                // Si hay un evento seleccionado, es una modificación
                modificarRecordatorio(selectedEvent.id, title);
            } else {
                // Si no hay evento seleccionado, es una creación
                agregarRecordatorio(title, dateSelect);
            }
        }
        modal.style.display = 'none'; // Ocultar el modal después de guardar
    });

    // Botón para eliminar
    deleteBtn.addEventListener('click', function () {
        if (confirm('¿Seguro que deseas eliminar este recordatorio?')) {
            eliminarRecordatorio(selectedEvent.id);
        }
        modal.style.display = 'none'; // Ocultar el modal después de eliminar
    });

    // Botón para cancelar
    cancelBtn.addEventListener('click', function () {
        modal.style.display = 'none'; // Ocultar el modal
    });
});

//Consumo del servicio getRecordatorio, actualizando el fullCalendar
function listarRecordatorios(successCallback, failureCallback) {
    fetch('http://127.0.0.1:5000/recordatorios')
        .then(response => response.json())
        .then(data => {
            const eventos = data.map(recordatorio => ({
                id: recordatorio.id,
                title: recordatorio.titulo,
                start: recordatorio.fecha,
                allDay: true
            }));
            successCallback(eventos);
        })
        .catch(error => {
            console.error('Error fetching events:', error);
            failureCallback(error);
        });
}

// Consumo de postRecordatorios
function agregarRecordatorio(titulo, fecha) {
    fetch('http://127.0.0.1:5000/recordatorios', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ id: Date.now(), titulo: titulo, fecha: fecha })
    })
        .then(response => response.json())
        .then(() => {
            console.log(`Se ha creado el recordatorio: ${titulo}`);
            calendar.refetchEvents();
        });
}

//Consumo del putRecordatorios modificando el titulo
function modificarRecordatorio(id, nuevoTitulo) {
    fetch(`http://127.0.0.1:5000/recordatorios/${id}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ titulo: nuevoTitulo })
    })
        .then(response => response.json())
        .then(() => {
            console.log(`Nuevo titulo para el recordatorio: ${id}`);
            calendar.refetchEvents();
        });
}

//Consumo de deteleRecordatorio
function eliminarRecordatorio(id) {
    fetch(`http://127.0.0.1:5000/recordatorios/${id}`, {
        method: 'DELETE'
    })
        .then(response => response.json())
        .then(() => {
            console.log(`Se elimino el recordatorio: ${id}`);
            calendar.refetchEvents();
        });
}

//Consumo del putRecordatorio modificando la fecha
function modificarFechaRecordatorio(id, nuevaFecha) {
    fetch(`http://127.0.0.1:5000/recordatorios/${id}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ fecha: nuevaFecha })
    })
        .then(response => response.json())
        .then(() => {
            calendar.refetchEvents();
        });

}
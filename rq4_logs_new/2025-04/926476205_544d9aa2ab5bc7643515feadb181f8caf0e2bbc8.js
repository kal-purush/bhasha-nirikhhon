document.addEventListener('DOMContentLoaded', function() {
    var socket = io();

    // Referencias al modal, input y botón
    var modal = document.getElementById('nickname-prompt');
    var nicknameInput = document.getElementById('nickname-input');
    var submitButton = document.getElementById('nickname-submit');

    // Mostrar el modal de nickname al cargar
    modal.style.display = 'block';

    // Función para cerrar el modal y emitir el evento de nuevo usuario
    function setNickname() {
        var nickname = nicknameInput.value || `Usuario${Math.floor(Math.random() * 1000)}`;
        modal.style.display = 'none'; // Ocultar modal
        socket.emit('new user', nickname); // Emitir nuevo usuario
    }
      
    // Evento para el botón de enviar
    submitButton.onclick = function() {
        setNickname();
    };

    // Evento para cuando se presiona enter en el input
    nicknameInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            setNickname();
        }
    });

    // Otros elementos y eventos...
    var form = document.getElementById('form');
    var input = document.getElementById('input');
    var messages = document.getElementById('messages');
    var userList = document.getElementById('user-list');
    var typingIndicator = document.getElementById('typing');

   
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        if (input.value) {
            socket.emit('chat message', input.value);
            input.value = '';
        }
    });

    input.addEventListener('input', function() {
        socket.emit('typing', nickname); 
    });

    socket.on('chat message', function(msg) {
        addMessage(msg);
        typingIndicator.textContent = '';
        document.getElementById('message-sound').play(); 
    });

    socket.on('message', function(msg) {
        addMessage(msg);
    });

    socket.on('update user list', function(users) {
        userList.innerHTML = '';
        users.forEach(function(user) {
            var item = document.createElement('li');
            item.textContent = user;
            userList.appendChild(item);
        });
    });

    socket.on('display typing', function(nickname) {
        typingIndicator.textContent = `${nickname} está escribiendo...`;
        clearTimeout(typingIndicator.timeout);
        typingIndicator.timeout = setTimeout(function() {
            typingIndicator.textContent = '';
        }, 1000);
    });

    function addMessage(msg) {
        var item = document.createElement('li');
        item.textContent = msg;
        messages.appendChild(item);
        item.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
});
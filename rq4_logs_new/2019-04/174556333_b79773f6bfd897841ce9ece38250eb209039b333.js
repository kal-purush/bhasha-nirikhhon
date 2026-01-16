document.addEventListener('DOMContentLoaded', () => {
    var socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port);
    

    // socket.on('connect', () => {
    //     console.log("Hello")
    // });
    
    document.querySelector('#channelForm').onsubmit = () => {
        const request = new XMLHttpRequest();
        const channel = document.querySelector('#channelInput').value;
        request.open('POST', '/channel');

        request.onload = () => {
            const data = JSON.parse(request.responseText);

            if (data.success) {

                // Creating a channel room and adding the proper class
                const a = document.createElement('a');
                a.innerHTML = channel;
                a.classList.add('room')
                a.setAttribute("href", "#");
                document.querySelector('ol').append(a);
                
                document.querySelector('#channelInput').value = '';
            } 

            else {
                console.log(data.error)
            }
    }
        const data = new FormData();

        data.append('channel', channel);
        request.send(data);
        return false;
    };
});
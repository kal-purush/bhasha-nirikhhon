const http = require('http');
const qrcode = require('qrcode-terminal');
const { Client } = require('whatsapp-web.js');
const axios = require('axios')

const country_code = '521';
const msg = 'hola, feliz cumpleaños ';

const client = new Client();


client.on('qr', (qr) => {
    qrcode.generate(qr, { small: true });
});

client.on('ready', async () => {
    console.log('client is ready!');
    try {
        const response = await axios.get('http://localhost:3000/task');
        //console.log(response.data);
        const amigos = response.data;
        const fecha = new Date();
        const day = fecha.getDate();
        const month = fecha.getMonth() + 1;
        const fechaHoy = day + '-' + month;
        console.log(fechaHoy);
        const result = amigos.filter(amigo => amigo.cumple === fechaHoy );
        if (result) {
            result.forEach(amigo => {
                let number = country_code + amigo.telefono + '@c.us';
                client.sendMessage(number, msg + amigo.nombre + '!!').then((response) => {
                    if (response.id.fromMe) {
                        console.log('Your message was sent');
                    }
                });
            });
        }

    } catch (error) {
        console.log('No hay amigos que cumplan años hoy')
        throw error
    }


});


client.initialize();


//console.log(fechaHoy);
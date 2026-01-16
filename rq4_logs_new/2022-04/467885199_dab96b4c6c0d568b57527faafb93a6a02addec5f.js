const FCM = require('fcm-node');
const { API_KEY, DEVICE_ID } = require('./keys');
const fcm = new FCM(API_KEY);

const message = {
    to: DEVICE_ID,
    priority: 'high',
    content_available: true,
    data: {
        params: {
            consultation_id: 1786,
            consultant_name: "Celia Creu",
            consultant_service: "Cardiología",
            consultant_id: 6369,
            phemium: true
        },
        text: "Llamada entrante",
        type: "CONSULTATION_CALL_REQUEST",
        color: "green"
    }
};

fcm.send(message, (err, response) => {
  if (err) {
    console.log(err);
    console.log('Something has gone wrong!');
  } else {
    console.log('Successfully sent with response: ', response);
  }
});
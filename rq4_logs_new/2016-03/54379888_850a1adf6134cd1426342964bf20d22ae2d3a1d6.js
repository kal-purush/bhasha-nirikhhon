'use strict'

var button = document.createElement('button')
button.innerHTML = 'Hello Bluetooth!'
document.body.appendChild(button)

button.addEventListener('click', () => {
  navigator.bluetooth.requestDevice({ filters: [{ services: ['battery_service'] }] })
  .then(device => {
    // Human-readable name of the device.
    console.log(device.name);
    // Indicates whether or not the device is paired with the system.
    console.log(device.paired);
    // Filtered UUIDs of GATT services the website origin has access to.
    console.log(device.uuids);

    // Attempts to connect to remote GATT Server.
    return device.connectGATT();
  }, error => {
    console.log(error)
  })
})
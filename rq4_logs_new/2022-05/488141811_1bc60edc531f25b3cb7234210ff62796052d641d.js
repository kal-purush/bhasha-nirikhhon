const EventEmitter = require('events')

const customEmmiter = new EventEmitter()

customEmmiter.on('response', (name, id) => {
    console.log(`Data received: user ${name}, id ${id}`)
})
customEmmiter.on('response', () => {
    console.log(`Some other logic here`)
})
customEmmiter.emit('response', 'Alex', 35)
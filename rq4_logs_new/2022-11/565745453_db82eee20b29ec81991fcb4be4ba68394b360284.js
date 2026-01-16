class Message {
    constructor(name , time , text){
        this.name = name,
        this.time = time,
        this.text = text
    }
    showText() {
        `${this.time}  ${this.name} : ${this.text}`
    }
}

class Messenger {
    constructor() {
        this.messages = []
    }
    show_history() {
        this.messages.map(message => message.toString())
    }
    send(name, text) {
        let message = new Message(name, text, new Date().getHours() + ' : ' + new Date().getMinutes())
        this.messages.push(message)
    }
}

let messenger = new Messenger()
messenger.send('Adil', 'ilk mesaj')
messenger.send('Məryəm', 'İkinci mesaj')
messenger.show_history()
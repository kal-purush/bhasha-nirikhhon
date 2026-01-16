import { Contacto } from './Contacto'

export class ContactList {
  // Creamos un array de tipo Contacto que almacenará la lista de contactos
  private contactos: Contacto[] = []

  addContact (contact:Contacto) {
    return this.contactos.push(contact)
  }

  findContactByName (name:string) {
    // Este metodo buscará el contacto deseado por medio de la clave nombre, y lo filtrará en un nuevo array donde sólo esté ese contacto
    return this.contactos.filter(contact => contact.nombre === name)
  }

  getAllContacts () {
    // Este método nos mostrará todos los contactos almacenados en la lista.
    return this.contactos
  }
}

const lista = new ContactList() // inicializamos la lista en una variable para poder usarla y también los métodos

const contacto1: Contacto = {
  nombre: 'Monte',
  correo_electronico: 'montesit00@gmail.com',
  telefono: '+54 9 370 412-3456'
}
lista.addContact(contacto1)

const contacto2: Contacto = {
  nombre: 'Chanti',
  correo_electronico: 'chantix04@gmail.com',
  telefono: '+54 9 370 423-4567'
}

lista.addContact(contacto2)

console.log(lista.findContactByName('Chanti'))
console.log('-----------------------------------------------------------------------------------')
console.log('-----------------------------------------------------------------------------------')
console.log('-----------------------------------------------------------------------------------')
console.log(lista.getAllContacts())
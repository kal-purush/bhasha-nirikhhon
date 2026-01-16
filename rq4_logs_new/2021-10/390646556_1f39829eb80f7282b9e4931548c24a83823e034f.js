export class Form {
    constructor(photograph) {
        this.photograph = photograph
    }

    /* Ouvre le formulaire */ 
    openForm(media) {
        const modal = document.querySelector('.modal')
        modal.style.display = 'block'

        const modalForm = document.querySelector('#modal-form')
        const profil = document.querySelector('form h1')
        const contact = document.querySelector('#photographer')
        const photographer = media

        contact.append(photographer)
        profil.append(contact)
        modalForm.append(profil)
    }

    /* Ferme le formulaire */ 
    closeForm() {
        const modal = document.querySelector('.modal')
        modal.style.display = 'none'
    }

}
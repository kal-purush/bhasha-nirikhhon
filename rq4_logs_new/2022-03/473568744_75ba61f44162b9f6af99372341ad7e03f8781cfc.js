const form = document.querySelector('.cta-form')


form.onsubmit = (e) => {
    if (!form.checkValidity()) {
        e.preventDefault()
        e.stopPropagation()
    }

    form.classList.add('was-validated')
    return false
}
function validateEntry() {
    
    var finalcontact = document.getElementById("contact").value
    var finalCompany = document.getElementById("company").value
    var finalJob = document.getElementById("job").value
    var finalEmail = document.getElementById("email").value
    
    var ContactAlert = "Thank you "+ finalcontact +" for your submission of an open position as a "+ finalJob +" with " + finalCompany +". I will reach out as soon as possible at "+ finalEmail + ". I am excited to possibly continue developing my skills with "+ finalCompany + " in the near future."

    alert(ContactAlert)
}




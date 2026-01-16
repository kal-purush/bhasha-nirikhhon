function createContactPage(divContent) {
    let contactDiv = document.createElement("div");

    let email = document.createElement("p");
    email.textContent = "email: commandershepard@codex.com";

    let address = document.createElement("p");
    address.textContent = "address: 123 Example Road, London, England";

    contactDiv.appendChild(email);
    contactDiv.appendChild(address);

    divContent.appendChild(contactDiv);
}

export {createContactPage};
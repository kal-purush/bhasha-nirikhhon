import * as storage from '../storage.js';

const form = document.getElementById("form");

function parseCustomerFromForm() {
    const customer_info = document.getElementsByClassName("customer-info");
    const customer = {};
    for (let field of customer_info) {
        if (field.type !== "radio") {
            let value = field.value;
            if (field.type === 'number') {
                value = parseInt(value)
            }
            customer[field.id] = value;
        }
    }
    customer["sex"] = document.querySelector('input[name="sex"]:checked').value;
    return customer;
}

form.addEventListener("submit", (e) => {
    const customer = parseCustomerFromForm();
    storage.save(customer);
    console.log(storage.load())
})




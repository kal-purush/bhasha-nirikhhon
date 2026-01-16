// Function to get query parameters
function getQueryParams() {
    const params = new URLSearchParams(window.location.search);
    return {
        image: params.get('image'),
        name: params.get('name'),
        details: params.get('details'),
        booking: params.get('booking')
    };
}

// Get the name from query parameters
const queryParams = getQueryParams();
const imageToDisplay = queryParams.image;
const nameToDisplay = queryParams.name;
const detailsToDisplay = queryParams.details;
const bookingToDisplay = queryParams.booking;

// Display the name in the h1 element
const imageDisplay = document.getElementById('image_display');
const hotel_details = document.getElementById('hotel_details')
const hname = document.getElementById('hname')
imageDisplay.innerHTML += `
<img src=${imageToDisplay} alt="">`
hotel_details.innerHTML += `
            <h2>${nameToDisplay}</h2>
            <p>${detailsToDisplay}</p><br/>
            <button class="submit">${bookingToDisplay}</button>`

hname.textContent += `${nameToDisplay}`

let btn = document.getElementsByClassName("submit")[0].addEventListener('click', (e) => {
    e.preventDefault()
    let form = document.getElementById("form");
    if (form.style.visibility === "visible") {
        form.style.visibility = "hidden"
    } else {
        form.style.visibility = "visible"
    }
})
let button = document.getElementsByClassName("button")[0];


button.addEventListener('click', (e) => {
    e.preventDefault();
    let firstName = document.getElementById("firstName").value;
    console.log(firstName);
    let lastName = document.getElementById("lastName").value;
    let email = document.getElementById("email").value;
    let phone = document.getElementById("PhoneNumber").value;

    let user = {
        hotelName: nameToDisplay,
        firstName: firstName,
        lastName: lastName,
        email: email,
        phone: phone
    }
    console.log(user);

    fetch("http://localhost:8080/savehot" , {
        method:"POST",
        headers: {
            'Content-Type':'Application/json'
        },
        body:JSON.stringify(user)
    }).then(Response =>{
        if(Response.ok){
            alert("Booked successfully")
            form.style.visibility = "hidden"
        }
    })
})
var openFormButton = document.getElementById('openFormButton');
        var formContainer = document.getElementById('formContainer');
        var myForm = document.getElementById('myForm');

        // Function to open the form
        openFormButton.onclick = function() {
            myForm.style.display = 'block';  // Show the form
            openFormButton.style.display = 'none';  // Hide the open form button
        };

        // Function to close the form and reset it
        myForm.onsubmit = function(event) {
            event.preventDefault();  // Prevent the default form submission behavior
            // alert('Form submitted: ' + document.getElementById('name').value);  // Show an alert with the entered name

            myForm.style.display = 'none';  // Hide the form
            openFormButton.style.display = 'block';  // Show the open form button
            myForm.reset();  // Reset the form fields
        };
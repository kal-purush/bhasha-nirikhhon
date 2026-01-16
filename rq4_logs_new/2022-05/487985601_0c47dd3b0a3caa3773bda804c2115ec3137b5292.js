require('dotenv').config()
const{CONNECTION_STRING} = process.env
const logInForm = document.querySelector("#logIn")
const createAccForm = document.querySelector("#createAcc")

const liPassword = document.querySelector("#liPassword")
const liEmail = document.querySelector("#liEmail")

const caEmail = document.querySelector("#caEmail")
const caPassword = document.querySelector("#caPassword")
const firstName = document.querySelector("#firstName")
const lastName = document.querySelector("#lastName")

const baseURL = "http://localhost:4111"

const handleLogIn = (e) => {
    e.preventDefault()

    //Check if any fields are empty
    //Check if email has an @ symbol
    if (liEmail < 1) {
        alert("You must enter an Email!")
        return
    } else if (liPassword < 1) {
        alert("You must enter a password!")
        return
    } else if (!liEmail.contains("@")) {
        alert("You must enter a valid email!")
        return
    }
    //Axios GET request to server to check if username and password match
    //Save the first name to the local storage 
    //Return by logging them in and moving them to the home if pass

    axios.get(`${baseURL}/login`)
        .then(res => {
            window.localStorage.setItem("first name", res.data.firstName)
            window.localStorage.setItem("userID", res.data.id)
            window.location.href = '../home.html'
        })

}

const handleCreateAcc = (e) => {
    e.preventDefault()
    
    //Check if any fields are empty
    //Check if email has an @ symbol
    if (caEmail < 1) {
        alert("You must enter an Email!")
        return
    } else if (caPassword < 1) {
        alert("You must enter a password!")
        return
    } else if (!caEmail.contains("@")) {
        alert("You must enter a valid email!")
        return
    } else if (firstName <1 ) {
        alert("You must enter a first name!")
        return
    } else if (lastName <1 ) {
        alert("You must enter a last name!")
        return
    }
    
    //Axios POST request to place the information into the DB
    //Save the first name to the local storage
    //Move them to the home screen
    axios.post(`${baseURL}/createAcc`)
        .then(res => {
            window.localStorage.setItem("first name", firstName)
            window.localStorage.setItem("userID", res.data.id)
            window.location.href = '../home.html'
        })
}

logInForm.addEventListener("submit", handleLogIn)
createAccForm.addEventListener("submit",handleCreateAcc)
const showUsers = (users) => {

    const OnlyFifteenUsers = users.slice(0,15)

    
    OnlyFifteenUsers.forEach((user) => {

        const { firstName, lastName, age, email, phone, image } = user
        const productContainer = document.createElement('div')
        productContainer.classList.add('product-container')
        

        const firstNameContainer = document.createElement('h1')
            firstNameContainer.innerText = `firstname: ${firstName}`
        
        const lastNameContainer = document.createElement('h1')
            lastNameContainer.innerText = `lastname: ${lastName}`

        const ageContainer = document.createElement('h1')
            ageContainer.innerText = `age: ${age}`

        const emailContainer = document.createElement('h1')
            emailContainer.innerText = `email: ${email}`

        const phoneContainer = document.createElement('h1')
            phoneContainer.innerText = `phone: ${phone}`

        const imageContainer = document.createElement('img')
        imageContainer.classList.add('image-container')
            imageContainer.src = image
           
            productContainer.append(imageContainer, firstNameContainer, lastNameContainer, ageContainer, emailContainer, phoneContainer )
            document.body.append(productContainer)
    })
}


const fetchUsers = (callback) => {
     fetch('https://dummyjson.com/users')
        .then((response) => response.json())
        .then((data) => callback(data.users))
}

fetchUsers(showUsers)
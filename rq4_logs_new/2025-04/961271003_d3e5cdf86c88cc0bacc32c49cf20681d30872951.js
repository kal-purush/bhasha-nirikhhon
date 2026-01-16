fetch( "https://restcountries.com/v3.1/all")
.then((respons)=>{
    return respons.json()
})
.then((data)=>{
    makeCountryList(data)
})
.catch((error)=>{
    console.log(error)
})

function makeCountryList(countries){
    // console.log(countries)

    const elements = countries.map((country)=>
        `<li class="country-item">
            <p class="country-name">${country.name.common}</p>
        </li>`
    ).join("")
    document.querySelector(".country-list").innerHTML = elements
}
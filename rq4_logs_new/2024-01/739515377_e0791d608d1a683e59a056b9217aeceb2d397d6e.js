function handleSearchSubmit(event){
    event.preventDefault();
    let searchInput = document.querySelector("#search-form-input");
    let cityElement = document.querySelector("#city");
    cityElement.innerHTML = searchInput.value;
}


let serchFormElement = document.querySelector("#search-form");
serchFormElement.addEventListener("submit", handleSearchSubmit);
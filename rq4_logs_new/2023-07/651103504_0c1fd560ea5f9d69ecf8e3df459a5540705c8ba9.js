// using html input, add an item to the array
// display the array to the list div shown in the html. Should be only the names, no commas
// when i click on one of the names in the list, it displays it to the item div
// CATCH, when you reference the name directly via the index value of the array to print to the dom, or creating an object where its attribute value is called "name" you must use something else to get it to display

// example: list[0] nor list[1].name cannot be used. You cannot create a different value instead of name, like list[2].cats
// you can use someting that is not just a string value. Maybe something like...a function?

const list =[{
    name: "Bob",
}]

const item = document.getElementById("item")
const listDiv = document.getElementById("displayList")
const form = document.getElementById("form")

form.addEventListener("submit", (event) => {
    event.preventDefault()
    console.log(event)
    const input = document.getElementById("name")
    const name = input.value
    list.push({name})
    input.value = ""
    displayList()
})

const displayList = () => {
    listDiv.innerHTML = ""
    list.forEach((item) => {
        const listItem = document.createElement("div")
        listItem.innerHTML = item.name
        listItem.addEventListener("click", () => {
            displayItem(item.name)
        })
        listDiv.appendChild(listItem)
    })
}

const displayItem = (name) => {
    item.innerHTML = name
}

displayList()
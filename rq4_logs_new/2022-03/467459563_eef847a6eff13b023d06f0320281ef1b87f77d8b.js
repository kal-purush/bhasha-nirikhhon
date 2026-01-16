const content = document.querySelector("#content")
const addBtn = document.querySelector("#add-btn")
const popupTab = document.querySelector("#popup")
const popupCloseBtn = document.querySelector("#popup-close-btn")
const typeContent = document.querySelector("#type-content")
const activeIcon = document.querySelector("#active-icon")
const filterItem = document.querySelector('#filter-item')

let itemType = ""
let itemName = ""
let itemStock = ""

let typeArray = [
    {
        type : "🧴"
    },
    {
        type : "🧂"
    },
    {
        type : "🍦"
    },
    {
        type : "🥕"
    },
    {
        type : "🧻"
    },
    {
        type : "🍸"
    },
    {
        type : "🍻"
    },
    {
        type : "🥔"
    },
    {
        type : "🥦"
    },
    {
        type : "🍭"
    },
    {
        type : "🥚"
    },
    {
        type : "🥫"
    },
    {
        type : "🍹"
    },
    {
        type : "🍞"
    },
    {
        type : "🍎"
    },
    {
        type : "🧀"
    },

]

const nameInput = document.getElementById("name-input")
const stockInput = document.getElementById("stock-input")
const popupAddBtn = document.getElementById("popup-add-btn")

function createBoard() {
    for (let i = 0; i < typeArray.length; i++){
        const typeIcon = document.createElement('h3')
        typeIcon.textContent = typeArray[i].type
        typeIcon.setAttribute ('data-id', i)
        typeIcon.addEventListener("click", choosenIcon)
        typeContent.append(typeIcon)

        const filterIcon = document.createElement('option')
        filterIcon.textContent = typeArray[i].type
        filterIcon.setAttribute('class','filter-icon')
        filterIcon.setAttribute('icon-id', i)
        filterIcon.setAttribute('sort-id', 1)
        
        filterItem.append(filterIcon)
    }
}
createBoard()

filterItem.addEventListener('change', filterStorage);

function choosenIcon(){
    let thisIconId = this.getAttribute('data-id')
    let thisIcon = typeArray[thisIconId].type
    //thisIcon.setAttribute('class','active-icon')
    itemType = thisIcon
    console.log(thisIcon)
}
let shelter = []
function filterStorage() {
    let thisFilterValue = this.value
    for (i = 0; i < itemArray.length; i++){
        if (itemArray[i].Type === thisFilterValue){
            console.log (itemArray[i].Type + " ini sama")

        }
        if (itemArray[i].Type != thisFilterValue){
            console.log (itemArray[i].Type + " ini tidak sama")
                

        }
    }
    //let selectedItem = 
}

addBtn.addEventListener("click", function(){
    popupTab.style.display = "block"
})

popupCloseBtn.addEventListener("click", function(){
    popupTab.style.display = "none"
})

// window.onbeforeunload = function() {
//     return "Dude, are you sure you want to leave? Think of the kittens!";
// }
let itemNumber = 0
let itemArray = []
popupAddBtn.addEventListener("click",function(){
    
    if (itemType == "" ){
        return alert ("Please Choose the item type ")
    }else if ( nameInput.value == "" || stockInput.value == ""){
        return alert ("Please Input item name and stock ")
    }
    itemName = nameInput.value
    itemStock = stockInput.value 
    itemNumber += 1
    let itemObj = {
        Type : itemType,
        Name : itemName,
        Stock : itemStock
    }

    itemArray.push(itemObj)
    localStorage.setItem(`items`,JSON.stringify(itemArray))
    createItem()
    popupTab.style.display = "none"
    stockInput.value = ""
    nameInput.value = ""
    itemType = ""
    
})
function createItem() {
        for (i = 0; i < itemArray.length; i++){
            console.log(itemArray)
            let items = document.createElement('div')
            items.setAttribute('class','item')
            let h1 = document.createElement('h1')
            h1.textContent = itemArray[i].Type
            let name = document.createElement('h3')
            name.textContent = itemArray[i].Name
            items.append(h1)
            items.append(name)
            content.append(items)
        
            let stock = document.createElement('div')
            stock.setAttribute('class','stock')
            let h3stock = document.createElement('h3')
            h3stock.textContent = itemArray[i].Stock
            h3stock.setAttribute ('id',i)
            stock.append(h3stock)
            content.append(stock)
            
            let sold = document.createElement('div')
            sold.setAttribute('class','sold')
            let soldInput = document.createElement('input')
            soldInput.setAttribute('id', i)
            soldInput.setAttribute('placeholder',0)
            soldInput.addEventListener("input", stockNow)
            let countBtn = document.createElement('button')
            countBtn.setAttribute('id', i)
            countBtn.innerText = "Count"
            countBtn.addEventListener("click", countStock)
            sold.append(soldInput)
            sold.append(countBtn)
            content.append(sold)
        
            let deleteDiv = document.createElement('div')
            deleteDiv.setAttribute('class','delete')
            let deleteBtn = document.createElement('button')
            deleteBtn.setAttribute('id', i)
            deleteBtn.setAttribute('class','delete-btn')
            deleteBtn.innerText = "X"
            deleteBtn.addEventListener('click',deleteItem)
            deleteDiv.append(deleteBtn)
            content.append(deleteDiv)        
            }
    
}

let thisValue = ""
function getItem() {
    if (localStorage.getItem("items") === null){
        itemArray = []
    } else {
        itemArray = JSON.parse(localStorage.getItem("items"))
    }

        for (i = 0; i < itemArray.length; i++){
        console.log(itemArray)
        let items = document.createElement('div')
        items.setAttribute('class','item')
        let h1 = document.createElement('h1')
        h1.textContent = itemArray[i].Type
        let name = document.createElement('h3')
        name.textContent = itemArray[i].Name
        items.append(h1)
        items.append(name)
        content.append(items)

        let stock = document.createElement('div')
        stock.setAttribute('class','stock')
        let h3stock = document.createElement('h3')
        h3stock.textContent = itemArray[i].Stock
        h3stock.setAttribute ('id',i)
        stock.append(h3stock)
        content.append(stock)
        
        let sold = document.createElement('div')
        sold.setAttribute('class','sold')
        let soldInput = document.createElement('input')
        soldInput.setAttribute('id', i)
        soldInput.setAttribute('placeholder',0)
        soldInput.addEventListener("input", stockNow)
        let countBtn = document.createElement('button')
        countBtn.setAttribute('id', i)
        countBtn.innerText = "Count"
        countBtn.addEventListener("click", countStock)
        sold.append(soldInput)
        sold.append(countBtn)
        content.append(sold)

        let deleteDiv = document.createElement('div')
        deleteDiv.setAttribute('class','delete')
        let deleteBtn = document.createElement('button')
        deleteBtn.setAttribute('id', i)
        deleteBtn.setAttribute('class','delete-btn')
        deleteBtn.innerText = "X"
        deleteBtn.addEventListener('click',deleteItem)
        deleteDiv.append(deleteBtn)
        content.append(deleteDiv)        
        }
    
}
getItem()

function stockNow() {
    thisValue = this.value
    console.log(thisValue)    
}
function countStock() {
    let thisButtonId = this.getAttribute('id')
    itemArray[thisButtonId].Stock = itemArray[thisButtonId].Stock - thisValue
        console.log(itemArray[thisButtonId].Stock)
        localStorage.setItem(`items`,JSON.stringify(itemArray))
        document.location.reload()
}
function deleteItem() {
    let thisItemId = this.getAttribute('id')
    itemArray.splice(thisItemId,1)
    localStorage.setItem("items",JSON.stringify(itemArray))
    document.location.reload()
    console.log(itemArray)
}

function clearStr() {
    localStorage.clear()
}
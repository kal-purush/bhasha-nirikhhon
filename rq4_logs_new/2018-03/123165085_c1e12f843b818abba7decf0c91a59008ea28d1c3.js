class ShoppingView {
    constructor(model) {
        model.subscribe(this.redrawList)
    }

    redrawList(shoppingList, msg) {
        let tbl = document.getElementById("shoppinglist")
        tbl.innerHTML = ""
        for (let item of shoppingList.newItems) {
            addRow(item, tbl)
        }
    }

}

function  addRow(item, parent) {
        let row = document.createElement("tr")
        row.classList.add(item.priority)
        let cb = document.createElement("input")
        cb.type = "checkbox"
        cb.classList.add("form-control")
        row.appendChild(cb)
        
        for (let val of ['name', 'quantity', 'store', 'section', 'price']) {
            let td = document.createElement("td")
            td.innerHTML = item[val]
            row.appendChild(td)
        }
        parent.appendChild(row)
    }
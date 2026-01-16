var data = [
    {
        qty: 1.5,
        description: 'Stock Item Example 0001',
        unitPrice: 1000,
        amount: 1500
    },
    {
        qty: 1,
        description: "Stock Item Example 0002",
        unitPrice: 2000.00,
        amount: 2000.00
    },
    {
        qty: 1,
        description: "Service Charge Invoicing Item 001",
        unitPrice: 100.00,
        amount: 100.00
    },
    {
        qty: 3,
        description: `Service Charge Invoicing Item 002<br/>
    Additional line 1 for this item<br/>
    Additional line 2 for this item`,
        unitPrice: 200,
        amount: 600
    }
]

function prepareDataTable() {
    $('#dataTable').html('')
    let subTotal = 0
    data.forEach(function (item) {
        let dataRow = `<tr>
            <td class="data r">${item.qty}</td>
            <td class="data">${item.description}</td>
            <td class="data r">${item.unitPrice}</td>
            <td class="data r">${item.amount}</td>
        </tr>`
        console.log({ dataRow })
        $('#dataTable').append(dataRow)
        subTotal += item.amount
    })
    let salesTax = subTotal * .1
    let totalDue = subTotal + salesTax
    
    $('#subTotal').html(subTotal)
    $('#salesTax').html(salesTax)
    $('#totalDue').html(totalDue)
    
}

$(document).ready(function () {
    let d = new Date()
    let dateStr = `${d.getDate()}-${d.getMonth()+1}-${d.getFullYear()}`
    $('#currentDate').html(dateStr)
    $('#addButton').click(function () {
        let qty = prompt('Quantity')
        let d = prompt('Description')
        let up = prompt('Unit Price')
        data.push({
            qty: qty,
            description: d,
            unitPrice: up,
            amount: qty * up
        })
    })
    prepareDataTable()
});

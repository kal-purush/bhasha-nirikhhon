//alert('hello')

function decrease(event) {
    let oldQuantity = $(event).prev().html();
    if (oldQuantity >= 1) {
        $(event)
            .prev()
            .html(parseInt(oldQuantity) - 1);
    }
    updateTotal();
}
function increase(event) {
    let oldQuantity = $(event).prev().prev().html();
    $(event)
        .prev()
        .prev()
        .html(parseInt(oldQuantity) + 1);
    updateTotal();
}

function remove(event) {
    // get index of row was clicked
    let index =
        parseInt(event.parentElement.parentElement.attributes.id.value) - 1;
    $("tbody")[0].removeChild($("tbody")[0].rows[index]);
    updateTotal;
}

function updateTotal() {
    let tbody = $("tbody")[0];
    let listRows = Array.from(tbody.rows); // convert HTMLcollection to array to use foreach
    //cacul tax
    listRows.forEach((elm) => {
        // get 2 point float
        let quantity = parseFloat(
            elm.cells[2].firstElementChild.textContent
        ).toFixed(2);
        let price = parseFloat(
            elm.cells[3].textContent.replace("$", "")
        ).toFixed(2);
        let value = Math.ceil((quantity * price * 12.5) / 100).toFixed(2);
        elm.cells[5].textContent = "$" + value;
    });

    //cacul total
    listRows.forEach((elm) => {
        let quantity = parseFloat(elm.cells[2].firstElementChild.textContent);
        let price = parseFloat(elm.cells[3].textContent.replace("$", ""));
        let discount = parseFloat(elm.cells[4].textContent.replace("$", ""));
        // when dont have discount
        discount = isNaN(discount) ? 0 : discount;
        let tax = parseFloat(elm.cells[5].textContent.replace("$", ""));
        let value = parseFloat(quantity * price - discount + tax).toFixed(2);
        elm.cells[6].textContent = "$" + value;
    });

    // cacul total price
    let tmp = 0;
    listRows.forEach((elm) => {
        let subtotal = parseFloat(elm.cells[6].textContent.replace("$", ""));
        tmp += subtotal;
    });
    $("#totalPrice").html("$" + tmp.toFixed(2));
    tmp = 0;
    //cacul total discount
    listRows.forEach((elm) => {
        let subdiscount = parseFloat(elm.cells[4].textContent.replace("$", ""));
        subdiscount = isNaN(subdiscount) ? 0 : subdiscount;
        tmp += subdiscount;
    });
    $("#totalDiscount").html("$" + tmp.toFixed(2));
    tmp = 0;
    // cacul total tax
    listRows.forEach((elm) => {
        let subtax = parseFloat(elm.cells[5].textContent.replace("$", ""));
        tmp += subtax;
    });
    $("#totalTax").html("$" + tmp.toFixed(2));
}

// call update when loading page
window.addEventListener("load", updateTotal());
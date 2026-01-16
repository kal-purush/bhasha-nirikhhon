/*jslint browser: true,
    devel: true,
    plusplus: true,
    maxerr: 1,
    indent: 4,
    maxlen: 85 */

window.onload = function () {
    "use strict";

    var button = document.getElementsByTagName("button")[0],
        qty,
        description,
        cost;

    function addRow() {
        qty = document.getElementById("qty").value;
        description = document.getElementById("description").value;
        cost = document.getElementById("cost").value;
        /* Add your code below this line. */

        /* Add your code above this line. */
    var tbody = document.getElementsByTagName("tbody")[0];
    var tr = document.createElement("tr");
    var tdQtyTextNode = document.createTextNode( qty );
    var tdQty = document.createElement("td");
    var tdDescriptionTextNode = document.createTextNode( description );
    var tdDescription = document.createElement("td");
    var tdCostTextNode = document.createTextNode( cost );
    var tdCost= document.createElement("td");

    tdQty.appendChild(tdQtyTextNode);
    tdDescription.appendChild(tdDescriptionTextNode);
    tdCost.appendChild(tdCostTextNode);
    tr.appendChild(tdQty);
    tr.appendChild(tdDescription);
    tr.appendChild(tdCost);
    tbody.appendChild(tr);

    }




    button.addEventListener("click", addRow, false);
};
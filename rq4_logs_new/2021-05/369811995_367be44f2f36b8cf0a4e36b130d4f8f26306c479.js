let id = 1;

document.getElementById("add").addEventListener('click', () =>{
    
    /* Pointer to the table of previously scanned values */
    let table = document.getElementById('db-values');

    /*Insert a new row at the bottom of the table  -1 adds at the end of the list */
    let row = table.insertRow(-1);

    /* Set the ID for this new row to a unique value.  Like a key to the table row entry */
    row.setAttribute('id', `item-${id}`);
    
    /*Fill the cells of this row with the data values entered on the form */
    row.insertCell(0).innerHTML = document.getElementById('transaction-date').value;
    row.insertCell(1).innerHTML = document.getElementById('item-code').value;
    row.insertCell(2).innerHTML = document.getElementById('location').value;
    row.insertCell(3).innerHTML = document.getElementById('quantity').value;

    /* Create a delete button and action and append it to the end of the current row */
    let actions = row.insertCell(4);
    actions.appendChild (createDeleteButton(id++));
    
    /* Reset defaults for the form fields */

    /* See if we can persist the transaction date, once set.    
    document.getElementById('transaction-date').value = '';
    */
    
    document.getElementById('item-code').value = '';
    document.getElementById('location').value = '';
    document.getElementById('quantity').value = '';
    
    /*  Set focus on the item code field so the user can loop through without having to constantly re-enter the date*/
    document.getElementById('item-code').focus();
});

function createDeleteButton(id){

    /* Create a row delete button */
    let btn = document.createElement('button');
    
    /* Set the button attributes */
    btn.className = 'btn btn-primary';
    btn.id = id;
    btn.innerHTML  = 'Delete';
    
    /* Set the onclick event for this button */

    btn.onclick = () => {
    
        /* Test output to  verify the correct row is being deleted */
        console.log (`Deleting row with id: item-${id}`);

        /* using the unique key, find the row */
        let elementToDelete = document.getElementById(`item-${id}`);
        
        /* Walk up to the parent node and remove the row */
        elementToDelete.parentNode.removeChild(elementToDelete);
    };

    /* return the button to the calling function */

    return btn;
}
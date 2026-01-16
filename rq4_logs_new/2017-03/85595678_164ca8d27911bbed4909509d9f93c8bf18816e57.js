function showModal(modal_id)
{
    document.getElementById(modal_id).style.display = "block";
}

function closeModal(evt)
{
    var modal_close_button = evt.target;
    var button_parent      = modal_close_button.parentElement;

    // Loop through parents until we get to modal div
    while (button_parent.className !== "modal")
    {
        button_parent = button_parent.parentElement;
    }

    button_parent.style.display = "none"; 
}

// Add event listeners for modal close buttons
var modal_close_buttons = document.getElementsByClassName('modal_close_button');

for (var i=0; i<modal_close_buttons.length; i++)
    modal_close_buttons[i].addEventListener('click', function(event) { closeModal(event) } );

// Add event listeners for modal open buttons
var modal_open_buttons = document.getElementsByClassName('modal_open_button');

for (var i=0; i<modal_open_buttons.length; i++)
{
    modal_open_buttons[i].addEventListener('click', function(event) 
    { 
        var modal_id = event.target.id.slice(0, event.target.id.indexOf('_open_button'));  
        showModal(modal_id);
    });
}
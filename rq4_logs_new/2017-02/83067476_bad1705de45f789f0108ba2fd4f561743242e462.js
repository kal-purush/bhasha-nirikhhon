var current_player = 0;
var x_vals = new Array(9);
var o_vals = new Array(9);

$( document ).ready(function() {
    for (i = 0; i < 9; i++) 
    {
        x_vals[i] = 0;
        o_vals[i] = 0;
    }
}); 

function move_player(current_player) {
    current_player++;
}

function check_mate() {

    for (i = 0; i < 9; i++) 
    {
        var value = $('#' + i).val();
        if (value) 
        {    
            console.log("THIS NEVER HAPPENDS");
            x_vals[i] = value;
        }
    }

    console.log(x_vals);
    console.log(o_vals);
}

function render(row) {
    var cross = '<span class="icon-cross"></span>';
    var circle = '<span class="icon-radio-unchecked"></span>';

    var row_id = $('#' + row);

    console.log("Habbala habbla");

    if (row_id.val()) 
    {
        return;
    }

    if (~current_player%2)
    {
        // X turn
        console.log(current_player);
        row_id.append(cross);
    }
    else
    {
        // O turn
        row_id.append(circle);
    }

    // Check for mate
    check_mate();
    
    console.log(row_id.val());
    current_player++;
    return;
}
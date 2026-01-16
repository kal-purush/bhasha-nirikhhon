function updateStamina( iNewStamina ) {
    document.getElementById( "bar" ).style.width = iNewStamina + "%";
}

Events.Subscribe( "updateStamina", function( iNewStamina ) {
    updateStamina( iNewStamina );
} )
var shape = {
    name: "rectangle",
    popup: function() {
 
        console.log('This inside popup(): ' + this.name);
 
        //setTimeout(function() { // items in this function don't have access to name.
        setTimeout( () => { // items in this function DO have access.
            console.log('This inside setTimeout(): ' + this.name);
            console.log("I'm a " + this.name + "!");
        }, 3000);
 
    }
};
 
shape.popup();
var colors = [
    "blue", "green", "orange", "brown", "umber", "cobalt", "sienna", "charcoal", "titanium white"
];

for(var i = 0; i<=colors.length-1; i++){
    var suffix = "";
    if ((i+1) === 1){var suffix = "st"}
    else if ((i+1) === 2){var suffix = "nd"}
    else if ((i+1) === 3){var suffix = "rd"}
    else{var suffix = "th"}
    console.log("My " + (i+1) + suffix + " choice is " + colors[i] + ".");
}
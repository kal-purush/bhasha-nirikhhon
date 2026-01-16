// Get all the dropdown from document
document.querySelectorAll(".better-dropdown-container").forEach(dropDownFunc);

// Dropdown Open and Close function
function dropDownFunc(dropDown) {
    console.log(dropDown.classList.contains("better-click-dropdown"));
    dropDown.addEventListener("click", function (e) {
        e.preventDefault();
        if (
            this.nextElementSibling.classList.contains(
                "better-dropdown-active"
            ) === true
        ) {
            // Close the clicked dropdown
            this.nextElementSibling.classList.remove("better-dropdown-active");
        } else {
            // add the open and active class(Opening the DropDown)
            this.nextElementSibling.classList.add("better-dropdown-active");
        }
    });
}

// Listen to the doc click
window.addEventListener("click", function (e) {
    // Close the menu if click happen outside menu
    if (e.target.closest(".better-menu") === null) {
        // Close the opend dropdown
        closeDropdown();
    }
});

// Close the openend Dropdowns
function closeDropdown() {
    // remove the open and active class from other opened Dropdown (Closing the opend DropDown)
    document.querySelectorAll(".better-dropdown-menu").forEach(function (menu) {
        menu.classList.remove("better-dropdown-active");
    });
}
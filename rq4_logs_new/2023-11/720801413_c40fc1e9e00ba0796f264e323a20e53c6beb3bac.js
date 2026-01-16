/**
 * Event handler for the "shown.bs.modal" event on the "#saerchModalets" element.
 * Handles the logic for displaying and hiding search results and search list.
 * @param {Event} e - The event object.
 */
$(document).on("shown.bs.modal", "#saerchModalets", function (e) {
    // Get the search input element
    const searchInput = document.querySelector(".search_query.form-control");

    // Get the search list element
    const searchList = document.querySelector(".saved_search__list");

    // Get the search name element
    const searchName = document.querySelector(".saved_search__name");

    // Get the saved search list outer element
    const saved_search__list__outer = document.querySelector(".saved_search");

    // Get the count of search names in the search list
    const searchNameCount = searchList.querySelectorAll(".saved_search__name").length;

    // Check if search list exists and if there are search names present
    if (searchList && searchNameCount > 0) {
        // Call the showhidesaves function to show/hide search results based on the search input value
        showhidesaves(searchInput.value);

        // Check if the search list height is greater than the search name height plus 5 pixels
        if (searchList.offsetHeight > searchName.offsetHeight + 5) {
            // Add the "showmore" class to the search list
            searchList.classList.add("showmore");

            // Set the height of the search list to the height of the search name
            searchList.style.height = `${searchName.offsetHeight}px`;

            // Create a button element
            const button = document.createElement("button");

            // Set the text content of the button to display the count of search names
            button.textContent = `Rozwiń wszystkie (${searchNameCount})`;

            // Add an event listener to the button to toggle the "showmore" class on the search list
            button.addEventListener("click", (e) => {
                searchList.classList.toggle("showmore");

                // Remove the button element from the DOM
                e.target.remove();
            });

            // Insert the button element after the search list
            searchList.insertAdjacentElement("afterend", button);
        }
    }

    // Add an event listener to the search input for the "input" event
    searchInput.addEventListener("input", function (e) {
        // Call the showhidesaves function to show/hide search results based on the search input value
        showhidesaves(e.target.value);
    });

    /**
     * Function to show or hide the saved search list based on the search input value.
     * @param {string} e - The search input value.
     */
    function showhidesaves(e) {
        if (e !== "") {
            // Remove the "shown" class from the saved search list outer element
            saved_search__list__outer.classList.remove("shown");
        } else {
            // Add the "shown" class to the saved search list outer element
            saved_search__list__outer.classList.add("shown");
        }
    }
});
document.addEventListener("DOMContentLoaded", function() {
    var button = document.getElementById("sub");
    var search = document.getElementById("search");

    button.addEventListener("click", function() {
        var input = document.getElementById("newTodoItem");
        var newValue = input.value;

        if (newValue.trim() !== "") {
            addTodoItem(newValue);
            input.value = "";
        }
    });

    search.addEventListener("input", function() {
        var query = search.value.toLowerCase();
        filterTodoItems(query);
    });

    function addTodoItem(itemText) {
        var ul = document.getElementById("options");
        var li = document.createElement("li");
        li.textContent = itemText;
        var deleteDiv = document.createElement("div");
        deleteDiv.className = "delete";
        deleteDiv.textContent = "✖";
        li.appendChild(deleteDiv);
        ul.appendChild(li);
        deleteDiv.addEventListener("click", function() {
            ul.removeChild(li);
        });
    }

    function filterTodoItems(query) {
        var items = document.querySelectorAll("#options li");
        items.forEach(function(item) {
            var text = item.textContent.toLowerCase();
            if (text.includes(query)) {
                item.style.display = "";
            } else {
                item.style.display = "none";
            }
        });
    }
    var existingDeleteButtons = document.querySelectorAll(".delete");
    existingDeleteButtons.forEach(function(button) {
        button.addEventListener("click", function(event) {
            var li = event.target.parentElement;
            li.parentElement.removeChild(li);
        });
    });
});
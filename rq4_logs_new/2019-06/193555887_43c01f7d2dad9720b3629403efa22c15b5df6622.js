// Save the reference to text-field into a variable. | <input type="text">
let shopitem = document.getElementById("item");
// Save the reference to new-item button into a variable. | <button>ADD ITEM</button>
let btn = document.getElementById("add");
// Save the reference to unordered or ordered list into a variable. | <ul></ul> or <ol></ol>
let ul = document.querySelector("#list");
// Save the reference to paragraph for feedback | <p class="feedback"></p>
let feedback = document.querySelector(".feedback");

console.clear();
// Start function addItem.
function shoppingList() {
    // Create list-item and store output in a variable.
    let listItem = shopitem.value;
    //trim white space
    let item = listItem.trim();
    // Check if user entered the value in input text-field.
    if (item.length !== 0) {
    // If so:
       // Use textContent property on created list-item
        let li= document.createElement('li');
       // and assign it with the value of the text written in the text-field
        li.innerText= listItem;
       // Append list item to unordered list.
        ul.appendChild(li);
       // Clear a feedback-message.
            feedback.innerText="";
       // Clear the text-field.
            shopitem.value="";
       // Put the cursor back to text-field
            shopitem.focus();
    // End if. 
    } else {
    // Otherwise:
    // Print the message nothing entered in the paragraph "feedback"
        feedback.innerText= "Nothing entered!";
    // End otherwise.
        }
// End function addItem.

    }
    // Register your function addItem for click event on button.
btn.addEventListener("click", shoppingList);
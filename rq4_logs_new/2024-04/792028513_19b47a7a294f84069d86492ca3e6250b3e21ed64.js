const inputBox = document.getElementById("input-box");
const listContainer = document.getElementById("list-container");

const li = document.createElement("li");
function addTask(){
    const task = inputBox.value.trim();
    li.innerHTML = `
    <span>${task}</span>
  <span class="delete-btn">Delete</span>
`;
    listContainer.appendChild(li);
    inputBox.value = "";
    const deleteBtn = li.querySelector(".delete-btn");
    deleteBtn.addEventListener("click", function () {
        if (confirm("Are you sure you want to delete this goal?")) {
          li.remove();
        }
      });
}
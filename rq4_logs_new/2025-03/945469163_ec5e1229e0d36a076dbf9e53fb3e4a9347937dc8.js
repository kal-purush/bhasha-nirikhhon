function adddata(event) {
    event.preventDefault();
    let name = document.getElementById("name").value;
    let age = document.getElementById("age").value;
    let email = document.getElementById("email").value;
    let gender = document.getElementById("gender").value;
    let department = document.getElementById("department").value;

    if (!name || !age || !email) {
        alert("Please fill all fields");
        return;
    }
    
    let table = document.getElementById("tablebody");
    let row = table.insertRow();
    row.innerHTML = `<td>${row.rowIndex}</td><td>${name}</td><td>${age}</td><td>${email}</td><td>${gender}</td><td>${department}</td><td><button class='delete-btn' onclick='deleteRow(this)'>Delete</button></td>`;
    document.getElementById("s-info").reset();
}

function deleteRow(btn) {
    let row = btn.parentNode.parentNode;
    row.parentNode.removeChild(row);
}
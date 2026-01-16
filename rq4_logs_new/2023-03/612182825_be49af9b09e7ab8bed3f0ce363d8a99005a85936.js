// Function to display all the data from local storage
function allData(){
    // Clear the table first
    table.innerHTML=``
    // Get the list of contacts from local storage
    contactList = JSON.parse(localStorage.getItem("listItem")) ??[] 

    // Loop through each contact and add it to the table
    contactList.forEach(function(value,i){
        // Get the table element
        var table = document.getElementById("table")

        // Add the contact data to the table
        table.innerHTML +=`
        <tr>
        <td>${i+1}</td>
        <td>${value.name}</td>
        <td>${value.stdClass}</td>
        <td>${value.tnum}</td>
        <td>${value.age}</td>
        <td>
           <button class="btn btn-sm btn-success" onclick="find(${value.id})">
             <i class="fa fa-edit"></i>
             </button>
         </td>   
        <td>
           <button class="btn btn-sm btn-danger" onclick="removeData(${value.id})">
             <i class="fa fa-trash"></i>
             </button>
         </td>    
       </tr>
        `
    })
}

// Function to save the contact data to local storage
function save(){
    // Get the list of contacts from local storage
    contactList = JSON.parse(localStorage.getItem("listItem")) ?? []

    // Get the id for the new contact
    var id
    contactList.length != 0 ? contactList.findLast((item) => id = item.id) : id = 0

    // Check if all fields are filled in
    if(!document.getElementById('name').value || !document.getElementById('age').value|| !document.getElementById('stdClass').value || !document.getElementById('tnum').value || !document.getElementById('rollNo').value){
        alert('Please fill in all fields')
        return
    }

    // If the id field is filled in, update the existing contact
    if(document.getElementById('id').value){
        contactList.forEach(value=>{
            if(document.getElementById('id').value == value.id){
                value.name = document.getElementById('name').value, 
                value.rollNo = document.getElementById('rollNo').value, 
                value.stdClass = document.getElementById('stdClass').value, 
                value.tnum = document.getElementById('tnum').value, 
                value.age = document.getElementById('age').value
            }
        });

        // Clear the id field
        document.getElementById('id').value = ''
    }else{
        // If the id field is not filled in, add a new contact
        var item = {
            id:id+1,

            name : document.getElementById('name').value, 
            rollNo :  document.getElementById('rollNo').value, 
            stdClass : document.getElementById('stdClass').value, 
            tnum : document.getElementById('tnum').value, 
            age : document.getElementById('age').value

        }

        // Add the new contact to the list
        contactList.push(item)
    }

    // Save the updated list to local storage
    localStorage.setItem('listItem',JSON.stringify(contactList))

    // Update the table with the new data
    allData()

    // Clear the form
    document.getElementById('form').reset()  
}

// This function finds a specific record in the localStorage array and populates the input fields with its values
function find(id){

    // Get the array from localStorage and parse it as JSON, or create an empty array if none exists
    contactList = JSON.parse(localStorage.getItem('listItem')) ?? []

    // Loop through the array and find the record with the matching ID
    contactList.forEach(function (value){
        if(value.id == id){
            // Populate the input fields with the record's values
            document.getElementById('id').value = value.id
            document.getElementById('name').value = value.name
            document.getElementById('stdClass').value = value.stdClass
            document.getElementById('rollNo').value = value.rollNo
            document.getElementById('tnum').value = value.tnum
            document.getElementById('age').value = value.age
        }
    })
}

// This function removes a specific record from the localStorage array and updates the table
function removeData(id){

    // Get the array from localStorage and parse it as JSON, or create an empty array if none exists
    contactList = JSON.parse(localStorage.getItem('listItem')) ?? []

    // Confirm with the user that they want to delete the record
    if(confirm("Are you sure you want to delete this record?")){
        // Remove the record with the matching ID from the array
        contactList = contactList.filter(function(value){
            return value.id !=id;
        });

        // Save the updated array back to localStorage
        localStorage.setItem('listItem',JSON.stringify(contactList))

        // Update the table to reflect the changes
        allData()
    }
}

allData()

function clearData(){
    document.getElementById('form').reset()
    document.getElementById('id').value = ""
}



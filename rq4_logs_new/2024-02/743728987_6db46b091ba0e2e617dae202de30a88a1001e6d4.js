// Paste here API Gateway URL
var API_URL = '';
function CreateTableFromJSON() {
    fetch(API_URL)
        .then(response => response.json())
        .then(data => {
            // Calculate total number of users
            var totalNumber = data.length;
            var textnode = document.createTextNode("Number of users: " + totalNumber);

            // Create list of headers for the table
            const headerList = ["Photo", "First Name", "Last Name", "Age", "Country"];
            // CREATE TABLE
            var table = document.createElement("table");

            // Create HTML table header

            var tr = table.insertRow(-1);                   // Table row

            for (var i = 0; i < headerList.length; i++) {
                var th = document.createElement("th");      // Table header
                th.innerHTML = headerList[i];
                tr.appendChild(th);
            }

            // Add Json data to the table as row
            for (var i = 0; i < data.length; i++) {
                tr = table.insertRow(-1);
                var tabCell = tr.insertCell(-1);
                var userPhoto = '<img src="'+ data[i].picture.medium + '">';
                tabCell.innerHTML = userPhoto;
                var tabCell = tr.insertCell(-1);
                tabCell.innerHTML = data[i].name.first;
                var tabCell = tr.insertCell(-1);
                tabCell.innerHTML = data[i].name.last;
                var tabCell = tr.insertCell(-1);
                tabCell.innerHTML = data[i].dob.age;
                var tabCell = tr.insertCell(-1);
                tabCell.innerHTML = data[i].location.country;
            }
            // Add newly create table to the container
            var divContainerNumber = document.getElementById("userNumber");
            divContainerNumber.innerHTML = "";
            divContainerNumber.appendChild(textnode);
            
            var divContainer = document.getElementById("userData");
            divContainer.innerHTML = "";
            divContainer.appendChild(table);
        });
}
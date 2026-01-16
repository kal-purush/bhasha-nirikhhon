document.addEventListener("DOMContentLoaded", function () {
    var xhr = new XMLHttpRequest();
    xhr.open("GET", "php/fetchview.php", true);
    xhr.onload = function () {
        var dataContainer = document.getElementById("data-container");
        dataContainer.innerHTML = xhr.responseText;

        customizeView();
    };
    xhr.send();
});

function customizeView() {
    var rows = document.querySelectorAll(".data-row");

    var dataTable = document.getElementById("data-table");

    rows.forEach(function (row) {
        var name = row.querySelector(".name").innerText;
        var band = row.querySelector(".band").innerText;
        var photoSrc = row.querySelector(".photo").src;


        var tableRow = dataTable.insertRow();

        var photoCell = tableRow.insertCell(0);
        var photoElement = document.createElement("img");
        photoElement.src = photoSrc;
        photoElement.classList.add("custom-photo");
        photoCell.appendChild(photoElement);

        var detailsCell = tableRow.insertCell(1);
        var detailsElement = document.createElement("div");
        detailsElement.classList.add("custom-details");

        var nameLabel = document.createElement("p");
        nameLabel.innerText = "Nama: " + name;

        var bandLabel = document.createElement("p");
        bandLabel.innerText = "Band: " + band;

        detailsElement.appendChild(nameLabel);
        detailsElement.appendChild(bandLabel);

        detailsCell.appendChild(detailsElement);
    });
}
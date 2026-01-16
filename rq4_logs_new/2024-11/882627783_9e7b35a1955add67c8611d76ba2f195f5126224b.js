document.addEventListener("DOMContentLoaded", function() {
    loadMenu();
    loadBranches();
    setupForm();
});

function loadMenu() {
    fetch("menu.xml")
        .then(response => {
            if (!response.ok) throw new Error("Network response was not ok");
            return response.text();
        })
        .then(data => {
            const parser = new DOMParser();
            const xmlDoc = parser.parseFromString(data, "application/xml");
            const menuContainer = document.getElementById("menu-container");
            const items = xmlDoc.getElementsByTagName("item");

            let output = "<ul class='menu-list'>";
            for (let i = 0; i < items.length; i++) {
                const name = items[i].getElementsByTagName("name")[0].textContent;
                const description = items[i].getElementsByTagName("description")[0].textContent;
                const price = items[i].getElementsByTagName("price")[0].textContent;
                const imageSrc = items[i].getElementsByTagName("image")[0].textContent;

                output += `
                    <li class="menu-item">
                        <img src="${imageSrc}" alt="${name}" class="menu-item-image">
                        <div class="menu-item-details">
                            <h3>${name}</h3>
                            <p>${description}</p>
                            <p class="price">${price}</p>
                        </div>
                    </li>`;
            }
            output += "</ul>";
            menuContainer.innerHTML = output;
        })
        .catch(error => {
            console.error("Error loading menu:", error);
            document.getElementById("menu-container").innerHTML = "<p>Sorry, the menu could not be loaded at this time.</p>";
        });
}

function loadBranches() {
    fetch("branches.xml")
        .then(response => {
            if (!response.ok) throw new Error("Network response was not ok");
            return response.text();
        })
        .then(data => {
            const parser = new DOMParser();
            const xmlDoc = parser.parseFromString(data, "application/xml");
            const branchesContainer = document.getElementById("branches-container");
            const branches = xmlDoc.getElementsByTagName("branch");

            let output = "<ul class='branches-list'>";
            for (let i = 0; i < branches.length; i++) {
                const address = branches[i].getElementsByTagName("address")[0].textContent;
                const contact = branches[i].getElementsByTagName("contact")[0].textContent;
                const hours = branches[i].getElementsByTagName("hours")[0].textContent;
                const mapLink = branches[i].getElementsByTagName("mapLink")[0].textContent;

                output += `
                    <li class="branch-item">
                        <p><strong>Address:</strong> ${address}</p>
                        <p><strong>Contact:</strong> ${contact}</p>
                        <p><strong>Opening Hours:</strong> ${hours}</p>
                        <a href="${mapLink}" target="_blank">Get Directions</a>
                    </li>`;
            }
            output += "</ul>";
            branchesContainer.innerHTML = output;
        })
        .catch(error => {
            console.error("Error loading branches:", error);
            document.getElementById("branches-container").innerHTML = "<p>Sorry, the branches information could not be loaded at this time.</p>";
        });
}

function setupForm() {
    const form = document.getElementById("contact-form");
    const responseMessage = document.getElementById("form-response");

    form.addEventListener("submit", function(event) {
        event.preventDefault();
        responseMessage.style.display = "block"; // Show confirmation message
    });
}
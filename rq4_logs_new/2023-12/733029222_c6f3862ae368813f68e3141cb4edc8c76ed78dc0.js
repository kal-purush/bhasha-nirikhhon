function fetchCameras() {
    fetch("cameras.json")
        .then(response => response.json())
        .then(data => displayData(data))
        .catch(error => console.error('Error fetching data:', error));
}

function displayData(data) {
    const table = document.getElementById('table');

    // Counter to keep track of the number of iframes added
    let iframeCount = 0;

    // Iterate through each camera in the JSON data
    for (const cameraKey in data) {
        if (data.hasOwnProperty(cameraKey)) {
            const camera = data[cameraKey];
            const iframe = document.createElement('iframe');
            iframe.src = camera.url;
            iframe.width = "800";
            iframe.height = "600";
            iframe.frameBorder = "0";
            iframe.allowFullscreen = false;

            // Create a new row for every two iframes
            if (iframeCount % 2 === 0) {
                table.insertRow();
            }

            // Add the iframe to the last cell of the current row
            const rowIndex = table.rows.length - 1;
            const cell = table.rows[rowIndex].insertCell();
            cell.appendChild(iframe);

            // You can also add other information about the camera if needed
            // For example, add a link to the camera location in the iframe
            const locationLink = document.createElement('a');
            locationLink.href = `https://www.google.com/maps?q=${camera.location},${camera.country}`;
            locationLink.textContent = `${camera.location}, ${camera.country}`;
            cell.appendChild(locationLink);

            // Increment the iframe count
            iframeCount++;

            // Break the loop if you've added four iframes (2x2 grid)
            if (iframeCount === 4) {
                break;
            }
        }
    }
}
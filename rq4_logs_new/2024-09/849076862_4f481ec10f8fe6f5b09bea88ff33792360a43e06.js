document.addEventListener('DOMContentLoaded', loadHistory);

function toggleHistoryDetails(index) {
    const detailsElement = document.getElementById(`historyDetails-${index}`);
    if (detailsElement.style.display === 'none') {
        detailsElement.style.display = 'block';
        detailsElement.style.animation = 'slideDown 0.5s ease-out';
    } else {
        detailsElement.style.animation = 'slideDown 0.5s ease-out reverse';
        setTimeout(() => {
            detailsElement.style.display = 'none';
        }, 500);
    }
}

function deleteHistoryItem(index) {
    let history = JSON.parse(localStorage.getItem('requestHistory') || '[]');
    history.splice(index, 1);
    localStorage.setItem('requestHistory', JSON.stringify(history));
    loadHistory();
}

function loadHistory() {
    const history = JSON.parse(localStorage.getItem('requestHistory') || '[]');
    displayHistory(history);
}

function displayHistory(history) {
    const historyContainer = document.getElementById('historyContainer');
    historyContainer.innerHTML = '';

    history.forEach((item, index) => {
        const historyItem = createHistoryItem(item, index);
        historyContainer.appendChild(historyItem);
    });
}

function createHistoryItem(item, index) {
    const historyItem = document.createElement('div');
    historyItem.className = 'history-item';
    historyItem.style.animationDelay = `${index * 0.1}s`;
    historyItem.innerHTML = `
        <div class="history-item-content">
            <div>
                <h3>${item.method} ${item.url}</h3>
                <p>Status: ${item.status}</p>
                <p>Time: ${item.time}</p>
                <p>Date: ${new Date(item.timestamp).toLocaleString()}</p>
            </div>
            <div class="history-item-buttons">
                <button onclick="duplicateRequest(${index})" class="glow duplicate-btn">Duplicate</button>
                <button onclick="toggleHistoryDetails(${index})" class="glow">Toggle Details</button>
                <button onclick="deleteHistoryItem(${index})" class="glow delete-btn">Delete</button>
            </div>
        </div>
        <div id="historyDetails-${index}" class="history-details" style="display: none;">
            <h3>Request Details</h3>
            <p><strong>Method:</strong> ${item.method}</p>
            <p><strong>URL:</strong> ${item.url}</p>
            <p><strong>Status:</strong> ${item.status}</p>
            <p><strong>Time:</strong> ${item.time}</p>
            <p><strong>Date:</strong> ${new Date(item.timestamp).toLocaleString()}</p>
            <h4>Request Body:</h4>
            <pre>${item.requestBody}</pre>
            <h4>Response Body:</h4>
            <pre>${item.responseBody}</pre>
        </div>
    `;
    return historyItem;
}

function searchHistory() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const history = JSON.parse(localStorage.getItem('requestHistory') || '[]');
    const filteredHistory = history.filter(item => 
        item.url.toLowerCase().includes(searchTerm)
    );
    displayHistory(filteredHistory);
}

function clearAllHistory() {
    if (confirm("This will clear the history. Are you sure?")) {
        localStorage.removeItem('requestHistory');
        loadHistory();
    }
}

function duplicateRequest(index) {
    const history = JSON.parse(localStorage.getItem('requestHistory') || '[]');
    const item = history[index];
    
  
    const url = new URL(item.url);
    const queryParams = {};
    url.searchParams.forEach((value, key) => {
        queryParams[key] = value;
    });

    const duplicatedState = {
        method: item.method,
        url: url.origin + url.pathname,
        body: item.requestBody,
        queryParams: queryParams,
        headers: item.headers || {} 
    };
    
    localStorage.setItem('duplicatedRequest', JSON.stringify(duplicatedState));
    window.location.href = 'index.html';
}
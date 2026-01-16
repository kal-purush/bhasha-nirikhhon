let players = [];
let currentRound = 0;

// Fetch scores from the server when the page loads
window.onload = async () => {
    await fetchScores();
};

async function fetchScores() {
    const response = await fetch('/api/scores');
    players = await response.json();
    updateScoreboard();
    document.getElementById('roundControls').style.display = players.length > 0 ? 'block' : 'none';
}

function addPlayer() {
    const name = document.getElementById('nameInput').value.trim();
    if (name) {
        fetch('/api/players', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
        })
        .then(response => response.json())
        .then(newPlayer => {
            players.push(newPlayer);
            updateScoreboard();
            document.getElementById('nameInput').value = '';
            document.getElementById('roundControls').style.display = 'block';
        });
    }
}

function updateScoreboard() {
    const tbody = document.querySelector('#scoreboard tbody');
    tbody.innerHTML = '';
    players.forEach((player, index) => {
        const row = tbody.insertRow();
        row.innerHTML = `
            <td>${player.name}</td>
            <td>${currentRound >= 1 ? `<input type="number" value="${player.round1 || 0}" onchange="updateScore(${index}, 1, this.value)">` : ''}</td>
            <td>${currentRound >= 2 ? `<input type="number" value="${player.round2 || 0}" onchange="updateScore(${index}, 2, this.value)">` : ''}</td>
            <td>${player.final || 0}</td>
            <td><button class="remove-btn" onclick="removePlayer(${index})">Remove</button></td>
        `;
    });
}

function updateScore(playerIndex, round, score) {
    fetch('/api/scores', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerIndex, round, score: parseInt(score) || 0 })
    })
    .then(response => {
        if (response.ok) {
            // Update the local players array and refresh the scoreboard
            players[playerIndex][`round${round}`] = parseInt(score) || 0;
            players[playerIndex].final = (players[playerIndex].round1 || 0) + (players[playerIndex].round2 || 0); // Update local final score
            updateScoreboard();
        }
    });
}

function removePlayer(index) {
    fetch(`/api/players/${index}`, { method: 'DELETE' })
    .then(response => {
        if (response.ok) {
            players.splice(index, 1);
            updateScoreboard();
        }
    });
}

function sortPlayers() {
    players.sort((a, b) => {
        const scoreA = currentRound === 1 ? (a.round1 || 0) : ((a.round1 || 0) + (a.round2 || 0));
        const scoreB = currentRound === 1 ? (b.round1 || 0) : ((b.round1 || 0) + (b.round2 || 0));
        return scoreB - scoreA;
    });
}

function startRound(round) {
    currentRound = round;
    document.querySelector(`button[onclick="startRound(${round})"]`).style.display = 'none';
    if (round === 1) {
        document.querySelector('button[onclick="startRound(2)"]').style.display = 'inline-block';
    } else if (round === 2) {
        document.querySelector('button[onclick="calculateFinalScores()"]').style.display = 'inline-block';
    }
    updateScoreboard();
}

function calculateFinalScores() {
    players.forEach(player => {
        player.final = (player.round1 || 0) + (player.round2 || 0);
    });
    sortPlayers();
    updateScoreboard();
}
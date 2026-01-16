document.addEventListener("DOMContentLoaded", async () => {
  const roomNumber = window.location.pathname.split("/")[2];
  const songImage = document.getElementById("song-image");
  const songTitle = document.getElementById("song-title");
  const songArtist = document.getElementById("song-artist");
  const playersContainer = document.querySelector(".players");

  let songs = [];
  let players = [];
  let correctPlayer = "";

  // Fetch songs and players from the server
  async function fetchGameData() {
    try {
      const response = await fetch(`/fetch-songs/${roomNumber}`);
      const data = await response.json();

      if (!data.success) {
        alert(data.message || "Failed to fetch game data");
        return;
      }

      songs = data.songs;
      players = data.players;

      displayPlayers();
      displayNextSong();
    } catch (error) {
      console.error("Error fetching game data:", error);
    }
  }

  // Display the list of players
  function displayPlayers() {
    playersContainer.innerHTML = "";
    players.forEach((player) => {
      const playerDiv = document.createElement("div");
      playerDiv.classList.add("player");
      playerDiv.innerHTML = `<h3>${player}</h3>`;
      playerDiv.addEventListener("click", () => handleGuess(player, playerDiv));
      playersContainer.appendChild(playerDiv);
    });
  }

  // Display the next song
  function displayNextSong() {
    if (!songs.length) {
      alert("Game Over!");
      return;
    }

    const song = songs.shift(); // Remove and get the next song
    correctPlayer = players[Math.floor(Math.random() * players.length)]; // Random correct player

    songImage.src = song.cover || "img/default-cover.jpg"; // Fallback if no image
    songTitle.textContent = "Guess the Song!";
    songArtist.textContent = song.artists;
  }

  // Handle a player's guess
  function handleGuess(player, playerDiv) {
    if (player === correctPlayer) {
      playerDiv.classList.add("correct");
      alert("Correct Guess!");
    } else {
      playerDiv.classList.add("incorrect");
      alert("Wrong Guess!");
    }

    // Delay and move to the next song
    setTimeout(() => {
      playerDiv.classList.remove("correct", "incorrect");
      displayNextSong();
    }, 2000);
  }

  await fetchGameData();
});
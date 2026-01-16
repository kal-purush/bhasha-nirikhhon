const leagueId = 4405; // Swiss Super League ID in TheSportsDB
const apiBase = 'https://www.thesportsdb.com/api/v1/json/3';
const dropdown = document.getElementById('spieltagDropdown');
const spieleTabelle = document.getElementById('spieleTabelle');

// Hole alle Spieltage und befülle Dropdown
fetch(`${apiBase}/eventsrounds.php?id=${leagueId}&s=2023-2024&r=1`)
  .then(res => res.json())
  .then(data => {
    const rounds = data.rounds || [];
    rounds.forEach(round => {
      const option = document.createElement('option');
      option.value = round;
      option.textContent = round;
      dropdown.appendChild(option);
    });
  });

// Zeige standardmäßig den nächsten Spieltag
zeigeSpiele('next');

dropdown.addEventListener('change', () => {
  const selected = dropdown.value;
  zeigeSpiele(selected);
});

// Spiele anzeigen
function zeigeSpiele(runde) {
  spieleTabelle.innerHTML = '';
  const url = runde === 'next'
    ? `${apiBase}/eventsnextleague.php?id=${leagueId}`
    : `${apiBase}/eventsround.php?id=${leagueId}&r=${encodeURIComponent(runde)}&s=2023-2024`;

  fetch(url)
    .then(res => res.json())
    .then(data => {
      const events = data.events || [];
      events.forEach(event => {
        const row = document.createElement('tr');
        const date = new Date(event.dateEvent + 'T' + (event.strTime || '12:00:00'));
        row.innerHTML = `
          <td>${date.toLocaleDateString()}</td>
          <td>${date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</td>
          <td>${event.strHomeTeam}</td>
          <td>${event.strAwayTeam}</td>
          <td>${event.strVenue || '-'}</td>
        `;
        spieleTabelle.appendChild(row);
      });
    })
    .catch(err => {
      console.error('Fehler beim Laden der Spiele:', err);
    });
}
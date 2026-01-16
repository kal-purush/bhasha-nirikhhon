const axios = require('axios');
const {headers, getCurrentDate, getFutureDate, formatDate} = require('./helpers');

async function printFixtures() {
  const today = getCurrentDate();
  const futureDay = getFutureDate(7);
  const url = `http://api.football-data.org/v2/competitions/2019/matches?dateFrom=${today}&dateTo=${futureDay}`;

  try {
    const res = await axios.get(url, headers);
    const matches = res.data.matches.map(match => ({
      time: formatDate(new Date(match.utcDate)),
      home: match.homeTeam.name,
      away: match.awayTeam.name
    }));
    console.table(matches);
  } catch (e) {
    console.log(e);
  }
}

module.exports = printFixtures;
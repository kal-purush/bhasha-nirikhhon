const fetch = require('node-fetch').default;

async function parseLink(link, i, arr) {
  console.log('Get', link);
  const kek = await fetch(link);
  const rez = await kek.json();
  console.log('Recieved', link);
  if (arr) {
    arr[i] = rez;
  }
  return rez;
}

async function getJsonViaPromise(id) {
  const obi = await parseLink(`https://swapi.co/api/people/${id}/`);
  try {
    await Promise.all([
      ...obi.films.map(parseLink),
      ...obi.vehicles.map(parseLink),
      ...obi.starships.map(parseLink),
      ...obi.species.map(parseLink),
      parseLink(obi.homeworld, 'homeworld', obi),
    ]);
  } catch (e) {
    return null;
  }
  return obi;
}

getJsonViaPromise(10).then(() => console.log('DONE!'));
console.log('END_TICK');
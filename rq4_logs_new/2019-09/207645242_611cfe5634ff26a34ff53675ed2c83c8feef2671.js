
const cassandra = require('cassandra-driver');

const { Uuid } = cassandra.types;


const { executeConcurrent } = cassandra.concurrent;
const faker = require('faker');


const fakeList = [];
const newPlaces = [];

const savedLists = [];
for (let i = 0; i < 10; i += 1) {
  fakeList.push(faker.lorem.word());
  const saveListName = {
    name: fakeList[i],
  };
  savedLists.push(saveListName);
}

for (let i = 1; i < 101; i += 1) {
  // randomly chooses true or false
  let plusVerified = true;
  if (Math.random() > 0.5) {
    plusVerified = false;
  }

  // randomly choost savedList
  const savedList = fakeList.slice(Math.floor(Math.random() * 20));

  // generates a places data
  const newPlace = {
    id: i,
    url: `https://mock-property-images.s3-us-west-1.amazonaws.com/houses/house-${i}.jpeg`,
    title: faker.lorem.sentence(),
    city: faker.address.city(),
    state: faker.address.state(),
    country: faker.address.country(),
    plusVerified,
    propertyType: faker.lorem.words(),
    price: Math.floor(Math.random() * 200 + 100),
    averageReview: Math.random() + 4,
    totalReviews: Math.floor(Math.random() * 100 + 100),
    savedList,
    about: faker.lorem.words(),
    theSpace: faker.lorem.words(),
    neighborhood: faker.lorem.words(),
  };
  newPlaces.push(newPlace);
}

// const numListings = 3;
// const listings = [];
// for (let id = 0; id < numListings; id += 1) {
//   const places = [];
//   const rand = Math.floor(Math.random() * 6 + 5);
//   for (let i = 0; i < rand; i += 1) {
//     places.push(newPlaces[Math.floor(Math.random() * 100)]);
//   }
//   listings.push({ id, places });
// }

const client = new cassandra.Client({ contactPoints: ['localhost'], localDataCenter: 'datacenter1', keyspace: 'nearby' });


// for (let i = 0; i < numListings; i += 1) {
//   const query = 'INSERT INTO listings JSON ?';
//   client.execute(query, [JSON.stringify(listings[i])])
//     .then((result) => {
//       // console.log(result);
//     });
// }

// const numListings = 1000000;
// const query = 'INSERT INTO listings JSON ?';
// let i = 0;
// const helperInsert = () => {
//   const places = [];
//   const rand = Math.floor(Math.random() * 6 + 5);
//   for (let j = 0; j < rand; j += 1) {
//     places.push(newPlaces[Math.floor(Math.random() * 100)]);
//   }
//   // console.log(JSON.stringify({ id, places }));
//   client.execute(query, [JSON.stringify({ id: i, places })])
//     .then(() => {
//       if (i < numListings - 1) {
//         i += 1;
//         // console.log(i);
//         if (i % 1000 === 0) {
//           console.log(i);
//         }
//         helperInsert();
//       }
//     });
// };
// helperInsert();

const numListings = 100000;
async function example() {
  await client.connect();

  // The maximum amount of async executions that are going to be launched in parallel
  // at any given time
  const concurrencyLevel = 32;
  const promises = new Array(concurrencyLevel);

  const info = {
    totalLength: numListings,
    counter: 0,
  };

  // Launch in parallel n async operations (n being the concurrency level)
  for (let i = 0; i < concurrencyLevel; i++) {
    promises[i] = executeOneAtATime(info);
  }

  try {
    // The n promises are going to be resolved when all the executions are completed.
    await Promise.all(promises);

    console.log(`Finished executing ${info.totalLength} queries with a concurrency level of ${concurrencyLevel}.`);
  } finally {
    client.shutdown();
  }
}

async function executeOneAtATime(info) {
  const query = 'INSERT INTO listings JSON ?';
  const options = { prepare: true, isIdempotent: true };

  while (info.counter++ < info.totalLength) {
    const places = [];
    const rand = Math.floor(Math.random() * 6 + 5);
    for (let j = 0; j < rand; j += 1) {
      places.push(newPlaces[Math.floor(Math.random() * 100)]);
    }
    if (info.counter % 10000 === 0) {
      console.log(info.counter);
    }
    const params = [JSON.stringify({ id: info.counter, places })];
    await client.execute(query, params, options);
  }
}

example();
const _ = require("lodash");

const testArrayOfObjects = [
  {
    id: 1,
    billItems: [
      {
        id: "1234",
        name: "Samosa",
        price: 25,
        category: "Snacks",
      },
      {
        id: "12345",
        name: "Dal Fry",
        price: 35,
        category: "Curry",
      },
      {
        id: "12346",
        name: "Allu Fry",
        price: 45,
        category: "Curry",
      },
      {
        id: "12347",
        name: "Bendi Fry",
        price: 25,
        category: "Curry",
      },
    ],
  },
  {
    id: 2,
    billItems: [
      {
        id: "1234",
        name: "Samosa",
        price: 25,
        category: "Snacks",
      },
      {
        id: "12345",
        name: "Dal Fry",
        price: 35,
        category: "Curry",
      },
    ],
  },
  {
    id: 3,
    billItems: [
      {
        id: "12345",
        name: "Dal Fry",
        price: 35,
        category: "Curry",
      },
      {
        id: "12346",
        name: "Allu Fry",
        price: 45,
        category: "Curry",
      },
      {
        id: "12347",
        name: "Bendi Fry",
        price: 25,
        category: "Curry",
      },
    ],
  },
  {
    id: 4,
    billItems: [
      {
        id: "12345",
        name: "Dal Fry",
        price: 35,
        category: "Curry",
      },
      {
        id: "12346",
        name: "Allu Fry",
        price: 45,
        category: "Curry",
      },
      {
        id: "12347",
        name: "Bendi Fry",
        price: 25,
        category: "Curry",
      },
      {
        id: "123467",
        name: "Allu Curry",
        price: 45,
        category: "Curry",
      },
      {
        id: "12347",
        name: "Bendi Pulusu",
        price: 25,
        category: "Curry",
      },
    ],
  },
];

// Only taking the required array from an array of objects...
const billItemsOnly = testArrayOfObjects.reduce((a, b) => {
  return [...a, ...b.billItems];
}, []);
//console.log(billItemsOnly);

// group by
const collectionsByName = _.groupBy(billItemsOnly, "name");
const collectionsByCategory = _.groupBy(billItemsOnly, "category");
//console.log(collectionsByName);
//console.log(collectionsByCategory);

const names = Object.keys(collectionsByName).map((name) => {
  //console.log(name);
});

//reduce

const array = [1, 2, 3, 4, 5, 6, 7, 8, 9];

const sum = array.reduce((a, b) => {
  return a + b;
}, 0);
//console.log(sum);

const ItemsArrayWithQuantity = [
  {
    id: 1,
    name: "Dal Fry",
    quantity: 3,
    price: 25,
  },
  {
    id: 2,
    name: "Bendi Fry",
    quantity: 2,
    price: 30,
  },
  {
    id: 4,
    name: "Allu curry",
    quantity: 1,
    price: 30,
  },
];

let newArray = [];

for (const item in ItemsArrayWithQuantity) {
  if (ItemsArrayWithQuantity[item].quantity === 1) {
    newArray.push(ItemsArrayWithQuantity[item]);
  } else {
    for (let i = 0; i < ItemsArrayWithQuantity[item].quantity; i++) {
      let obj = {
        id: ItemsArrayWithQuantity[item].id,
        name: ItemsArrayWithQuantity[item].name,
        price: ItemsArrayWithQuantity[item].price,
      };
      newArray.push(obj);
    }
  }
}


const grouping=_.groupBy(newArray,'name')
//console.log(grouping);

// for names 

Object.keys(grouping).map((item)=>{
  console.table(item);
})


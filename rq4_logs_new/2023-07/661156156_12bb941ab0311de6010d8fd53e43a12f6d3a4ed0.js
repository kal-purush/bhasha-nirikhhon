// npm init -y 
// npm install csv-parser fs
// the csv-parser is to read whereas fs is to write
// For more details: https://www.npmjs.com/package/csv-parser 

const fs = require('fs');
const csv = require('csv-parser');


//1. Write a script that extracts and lists Name and FavoriteFridge1.
const extractNameAndFavoriteFridge1 = () => {

const results = [];

fs.createReadStream('fileA.csv')
  .pipe(csv({ headers: ['Name', 'FavoriteFridge1'] })) // extracting the headers we need
  .on('data', (data) => {
    const name = data.Name;
    const favoriteFridge1 = data.FavoriteFridge1;
    results.push({ name: name, favoriteFridge1: favoriteFridge1 });
  })
  .on('end', () => {
    // formatted output
    const output = results.map((entry) => `${entry.name}, ${entry.favoriteFridge1}`);
    const outputString = output.join('\n'); // Join the array elements with newline characters
    const header = '***************Task 1****************\n';

    fs.appendFile('fileB.csv', header + outputString, (err) => {
        if (err) {
          console.error(err);
          return;
        }
        console.log('Task 1 complete! Results saved to fileB.csv');
      });

    console.log(output)
  });
}

// 2. Write a script that will extract and list Jill, Candice, and Alycia’s items and create a column for total cost.
const extractItemsAndTotalCost = () => {
    const results = new Map();

    fs.createReadStream('fileA.csv')
      .pipe(csv({ headers: ['Name', 'FavoriteFridge1', 'Items', 'Cost', 'FavoriteFridge2'] }))
      .on('data', (data) => {
        const name = data.Name;
        const items = data.Items;
        const cost = parseFloat(data.Cost);
  
        if (!isNaN(cost) && cost > 0 && (name === 'Jill' || name === 'Candice' || name === 'Alycia')) {
          if (!results.has(name)) {
            results.set(name, { name: name, items: [], totalCost: 0 });
          }
  
          const person = results.get(name);
          person.items.push(items);
          person.totalCost += cost;
        }
      })
      .on('end', () => {
        const output = Array.from(results.values()).map((entry) => {
          const totalCost = entry.totalCost;
          return `${entry.name}, Items: ${entry.items.join(', ')}, Total Cost: ${totalCost}`;
        });
  
        const header = '\n*************Task 2**************\n';
        const content = output.join('\n');
  
        fs.appendFile('fileB.csv', header + content, (err) => {
          if (err) {
            console.error(err);
            return;
          }
          console.log(output)
          console.log('Task 2 complete! Results saved to fileB.csv');
        });
      });
  };

// 3. Write a script that can be used to update the CSV to change all FavoriteFridge1 to FavoriteFridge2 at 11pm on Sunday 12 January 2019.
const updateCSV = () => {

    const currentDate = new Date();
  const targetDate = new Date('2019-01-13T03:00:00'); // 11pm on Sunday 12 January 2019 in UTC
  
  if (currentDate > targetDate) {

    const updatedData = [];

    fs.createReadStream('fileA.csv')
      .pipe(csv())
      .on('data', (data) => {
        const { FavoriteFridge1, FavoriteFridge2 } = data;
        if (FavoriteFridge1) {
          data.FavoriteFridge1 = FavoriteFridge2;
        }
        updatedData.push(data);
      })
      .on('end', () => {
        const output = updatedData.map((data) => Object.values(data).join(',')); // new array for csv file

        const header = '\n***********Task 3**********\n';
        const content = output.join('\n');
        
        fs.appendFile('fileB.csv', header + content, (err) => {
          if (err) {
            console.error(err);
            return;
          }
          console.log('Task 3 - updated! Results saved to fileB.csv');
        });
      });
    }
  };

  extractNameAndFavoriteFridge1()
  extractItemsAndTotalCost()
  updateCSV()
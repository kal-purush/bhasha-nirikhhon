
/**
 * Prototype Function to add days
 * Credits:
 * http://stackoverflow.com/questions/563406/add-days-to-javascript-date
 * @param int days Number of days
 */
Date.prototype.addDays = function(days) {
  var dat = new Date(this.valueOf());
  dat.setDate(dat.getDate() + days);
  return dat;
}

// Interval years
const period = { 'begin': 2000, 'end': 2200 };
const fileOutput = process.argv[2] || 'output.csv';
const fs = require('fs');

let dates = { // as many as you want
  'key': {
    'day': 0, // sun = 0, mon = 1, tue = 2, wed = 3, thu = 4, fri = 5, sat = 6
    'month': 5, // 1 = Jan, 2 = Feb [...]
    'order': 2, // *nd occurrence in the Month (e.g: If 3rd Sunday, use order = 3)
    'dates': [], // leave it blank and the script will fill it
  }
}

try {
  for (let obj in dates) {
    for (let year = period.begin; year < period.end; year++) {
      dates[obj].dates.push( calc( year, dates[obj].month, dates[obj].day, dates[obj].order ) );
    }
  }
  output( fileOutput, dates );
  console.log('File saved in ' + fileOutput);
} catch (e) {
  console.warn('There was an error: ' + e);
}

/**
 * Function to get the orderObj from
 * an event
 * @param  {int} year      [description]
 * @param  {int} month      [description]
 * @param  {int} day      [description]
 * @param  {int} orderObj [description]
 * @return {String}       Retruns first date of the
 * event in the year
 */
function calc ( year, month, day, orderObj ) {
  let d = new Date ( year, (month - 1), 1 );
  let order = 0;
  while (order != orderObj) {
    if (d.getDay() == day) {
      order++;
    }
    d = (order != orderObj) ? d.addDays(1) : d;
  }
  return d.getFullYear() + '-' + ('0' + d.getMonth()).slice(-2) + '-' + ('0' + d.getDate()).slice(-2);
}

/**
 * Function to output data do file
 * @param  {String} fileName String to output file with extension
 * @param  {Mixed} data      Array
 */
function output ( fileName, data ) {
  let content = '';
  for (let d in dates) {
    for (let i in dates[d]['dates']) {
      content += d + ';' + dates[d]['dates'][i] + '\n';
    }
  }
  fs.writeFileSync(fileName, content, 'utf8');
}
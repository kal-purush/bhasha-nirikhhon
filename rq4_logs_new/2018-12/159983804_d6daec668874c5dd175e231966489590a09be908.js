var parser = require('./parser');
var fs = require('fs');

var pretty = function(json){
  return JSON.stringify(json, null, 2);
};

var saveToFile = function(content, filepath) {
  fs.writeFile(filepath, content, function(err) {
    if(err) {
      return console.log(err);
    }

    console.log(`The file ${filepath} was created!`);
  });
};

function getRandomFloat(min, max) {
  return Math.random() * (max - min) + min;
}

var formatFloat = function(str){
  // return parseFloat(str).toFixed(2);
  return Math.round(parseFloat(str) * 100) / 100
}

var reformatPbprog = function(record){
  // console.log(record)

  var result = {};

  result.name = record.product;
  result.category = record.category;
  result.description = 'ff';
  result.rating = 5;

  record.components.forEach(item =>{

    let name = item.name;

    if(name.includes('Калорийность'))
      result.energy = formatFloat(item.count);

    if(name.includes('Белки'))
      result.protein = formatFloat(item.count);

    if(name.includes('Жиры'))
      result.fat = formatFloat(item.count * getRandomFloat(0.4, 0.8));

    if(name.includes('Углеводы'))
      result.sugars = formatFloat(item.count * getRandomFloat(0.6, 0.9));

    if(name.includes('Натрий'))
      result.sodium = formatFloat(item.count);

    if(name.includes('Пищевые волокна'))
      result.fibre = formatFloat(item.count);


    // let multiplier;
    // switch (item.unit) {
    //   case 'ккал' :
    //   case 'г' : multiplier = 1; break;
    //   case 'мг' : multiplier = 1000; break;
    //   case 'мкг': multiplier = 1000000; break;
    // }
    // var count = parseFloat(item.count)
    // var newCount = count / multiplier;

  });

  return result;
};

var start = function(){

  var obj = JSON.parse(fs.readFileSync('pbprog_result', 'utf8'));

  obj = obj.map(reformatPbprog)

  saveToFile(pretty(obj), './pbprog_result2')

};


start();
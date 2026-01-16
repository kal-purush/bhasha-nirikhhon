
//run using node one.js
const fs = require('fs');
fs.readFile('./phase1-list.csv', 'utf8', (err, data) => {
  if (err) throw err;
  var arr = data.split(',');
  var loadedJsonObject = require('./final-json-template.json');
  var jsonObject = loadedJsonObject;
  var arrOfJsonObjects = [];
  var xml = "";
  for(var x = 0; x < arr.length; x++){
      if(arr[x].indexOf('object') >= 0){ //check if a line contains the word 'object' and is therefore an object
        const stringToBeParsed = arr[x].substring(arr[x].indexOf(` ""object|`) + 3, arr[x].length - 2);
        const arrayToBeParsed = stringToBeParsed.split('|');

        jsonObject.annotations.objects[0].features.gender = arrayToBeParsed[1];
        jsonObject.annotations.objects[0].features.major_category = arrayToBeParsed[2];
        jsonObject.annotations.objects[0].features.sub_category = arrayToBeParsed[3];
        if(arrayToBeParsed[4]) //if the line contains a specific_category prop, fill it
          jsonObject.annotations.objects[0].features.specific_category = arrayToBeParsed[4];

        const topRightX = parseInt(arr[x + 1].substring(arr[x + 1].indexOf(': [' ) + 3, arr[x].length));
        const topRightY = parseInt(arr[x + 2].substring(0, arr[x + 2].length - 1));
        const bottomLeftX = parseInt(arr[x + 3].substring(arr[x + 3].indexOf(': [' ) + 3, arr[x].length));
        const bottomLeftY = parseInt(arr[x + 4].substring(0, arr[x + 4].length - 1));

        /* Population xmin, ymin, xmax, and ymax */
        jsonObject.annotations.objects[0].bbox.xmin = bottomLeftX;
        jsonObject.annotations.objects[0].bbox.ymin = bottomLeftY;
        jsonObject.annotations.objects[0].bbox.xmax = topRightX;
        jsonObject.annotations.objects[0].bbox.ymax = topRightY;

        jsonObject.global.remote_location = 'http://s3.amazonaws.com/markable-training-data/image-files-2017-jpg/';
        arrOfJsonObjects.push({json: jsonObject, xml: jsonToXML(jsonObject)});
        jsonObject = loadedJsonObject;
        xml = "";
      }
      else if(arr[x].indexOf('human') >= 0){ //population for human
        const humanTopRightX = parseInt(arr[x + 1].substring(arr[x +1].indexOf(': [') + 3, arr[x + 1].length));
        const humanTopRightY = parseInt(arr[x + 2].substring(0, arr[x + 2].length - 1));
        const humanBottomLeftX = parseInt(arr[x + 3].substring(arr[x + 3].indexOf(': [') + 3, arr[x + 3].length));
        const humanBottomLeftY = parseInt(arr[x + 4].substring(0, arr[x + 4].length - 1));

        jsonObject.annotations.humans[0].bbox.xmin = humanBottomLeftX;
        jsonObject.annotations.humans[0].bbox.ymin = humanBottomLeftY;
        jsonObject.annotations.humans[0].bbox.xmax = humanTopRightX;
        jsonObject.annotations.humans[0].bbox.ymax = humanTopRightY;

        jsonObject.global.remote_location = 'http://s3.amazonaws.com/markable-training-data/image-files-2017-jpg/';
        arrOfJsonObjects.push({json: jsonObject, xml: jsonToXML(jsonObject)});
        jsonObject = loadedJsonObject;
        xml = "";
      }
  }
  return arrOfJsonObjects;
});

function jsonToXML(json){ //jsonToXML conversion
  var answer = "";
  for(var key in json){
    if(json.hasOwnProperty(key) == false || json[key] == undefined)
      continue;

    if (typeof(json[key]) == 'object') {
      answer += `<${key}>` + jsonToXML(new Object(json[key])); //for objects
    }
    else{ //final value
      answer += (`<${key}>` + json[key]);
    }
    answer += `</${key}>`;
  }
  return answer;
}
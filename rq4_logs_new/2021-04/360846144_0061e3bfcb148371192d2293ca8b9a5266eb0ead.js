const fs = require('fs');
const utils = require('./utils');

//Read input data
let rawdata = fs.readFileSync('inputData.json');
let jsonData = JSON.parse(rawdata);

//Main Function to report BMI details to output.txt
const reportBMI = (data) => {
const bmiArray = utils.calculateBMI(data);
const result = utils.mapBMI(bmiArray);
var count = 0;
var stream = fs.createWriteStream("output.txt");
stream.once('open', () => {
    stream.write("Gender \t Height \t Weight \t BMI \t Health Risk \t  BMI Category \n");
    for(let i=0;i<bmiArray.length;i++){
       stream.write(jsonData[i].Gender+" \t "+jsonData[i].HeightCm+" \t     "+jsonData[i].WeightKg+" \t     "+bmiArray[i]+" \t "+result[i]["Health Risk"]+"\t     "+result[i]["BMI Category"]+"\n");
      if(result[i]["BMI Category"] == "Overweight"){
        count = count + 1;
      }
    }
    stream.write("Total number of Overweight pepole: "+ count+"\n")
    stream.end();
  });
  console.log("BMI Resport has been generated successfully in output.txt file");
}

reportBMI(jsonData);
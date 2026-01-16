const app = require('express')();
let csv = require('csv');
let path = require('path');
var obj = csv();
function MyCSV(fone,ftwo,fthree,ffour,ffive,fsix){
	this.id = fone;
	this.empno = ftwo;
	this.empname = fthree;
	this.salary = ffour;
	this.department = ffive;
	this.deisgnation = fsix;
};
// Define the MyCSV object with parameterized constructor, this will be used for storing the data read from the csv into an array of MyCSV. You will need to define each field as shown above.



var MyData = []; 

// MyData array will contain the data from the CSV file and it will be sent to the clients request over HTTP. 



csv.from.file('./MyReport.csv').to.array(function (data) {

    for (var index = 0; index < data.length; index++) {

        MyData.push(new MyCSV(data[index][0], data[index][1], data[index][2]));

    }
    console.log(MyData);

});
app.listen(8000,(error,success)=>{
	if(error){
		console.log("Error while starting server");
	}
	console.log("Server started");
});
var inquirer=require('inquirer');

//connects to the Bamazon_db database
var mysql=require('mysql');
var connection=mysql.createConnection({
	host:"localhost",
	port: 4000,
	user:"root", // username
	password:"", // password
	database:"Bamazon_db"})

//shows if you're connected or not
connection.connect(function(err){
	if(err) throw err;
	console.log("connected as id "+connection.threadID);
	console.log();})

//Display All Items available for sale
connection.query('SELECT * FROM Bamazon_db.products;',function(err,res){
	for(var i=0;i<res.length;i++){
		console.log(res[i].ItemID+" | "+res[i].ProductName+" | "+res[i].Price+" | "+res[i].StockQuanitiy);
		}
	console.log("-----------------------------------");
});

//2 messages

//First message = ID of the product they would like to buy

//Second message = how many of the product they would like to buy

// Check if your store has enough quantity of the product to meet the customer's request. If not, you should respond to the user by saying: "Insufficient quantity" and prevent the order from going through.

// If your store DOES have enough of the product to meet the customer's request, you should fulfill their order. This means that you should show them the total cost of their puchase. Then update the SQL database to reflect the remaining quantity.

//The following is from class to use for this assignment

inquirer.prompt([{
   type: "list",
   name: "option",
   message: "What product would you like to buy?",
   choices: [
       "Medal",
       "Hose",
       "The Catcher in the Rye",
       "Cutter",
       "i-Phone",
       "Go Pro Camera",
       "Play Station4",
       "Printer",
       "Wooden spindle",
       "Tshirt",
       "Shoes",
       "Pens",
   ]
}]).then(function(user) {
   console.log(user.option);

   if (user.option == 'Medal') {
       inquirer.prompt([{
           type: 'input',
           message: 'How many units do you want to purchase?',
           name: 'numberItem'
       }]).then(function(item) {
           var post = { ProductName: item.Medal, StockQuanitiy: item.numberItem }
           con.query('Select * from products * where ProductName set Medal', post, function(err, result) {
               if (err) throw err;
           });
           con.query('SELECT * from products order by ItemID DESC limit 1', function(err, result) {
               if (err) throw err;
               console.log(result);
           });
       })
   } 
   else if (user.option == 'Hose') {
       inquirer.prompt([{
           type: 'input',
           message: 'How many do you want to purchase?',
           name: 'numberItem'
       }]).then(function(item) {
           var post = { ProductName: item.Hose, StockQuanitiy: item.numberItem }
           con.query('INSERT INTO products set ?', post, function(err, result) {
               if (err) throw err;
           });
           con.query('SELECT * from products order by ItemID DESC limit 1', function(err, result) {
               if (err) throw err;
               console.log(result);
           });
       })
   } 
   else if (user.option == 'Cutter') {
       inquirer.prompt([{
           type: 'input',
           message: 'How many do you want to purchase?',
           name: 'numberItem'
       }]).then(function(item) {
           var post = { ProductName: item.Cutter, StockQuanitiy: item.numberItem }
           con.query('INSERT INTO products set ?', post, function(err, result) {
               if (err) throw err;
           });
           con.query('SELECT * from products order by ItemID DESC limit 1', function(err, result) {
               if (err) throw err;
               console.log(result);
           });
       })
   } 
   else {
       con.query('select * from products', function(err, result) {
           if (err) throw err;
           console.log(result);

});
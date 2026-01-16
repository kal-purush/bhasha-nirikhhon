require("dotenv").config();
// Load the NPM Package inquirer
var inquirer = require("inquirer");
var mysql = require("mysql");
var console_table = require("console.table");

// object variable containing settings for connecting to MySQL database
var connection = mysql.createConnection({
    host: 'localhost',
    //The port from MySQL
    port: 3306,
    //My username
    user: "root",
    //My password that's housed in .env file
    password: process.env.password, 
    database: 'bamazon'
});

// start database connection && connection successful
connection.connect(function(err) {
    if (err) throw err; 
    console.log('Successful connection! ' + connection.threadId); //threadid is a process id from mySQL 
    //call function to display the products
    displayProducts();
}) // closing connection function

//query the database for all of the products available for sale (ids, name, and prices)
function displayProducts() {
    console.log("Hello && Thanks for shopping @ Bamazon!")
    connection.query("SELECT * FROM products", function(err,res) {
        if (err) throw err;
        console.table(res)
        displayTable(res);
        makePurchase();
    });
}

function displayTable(res) {
    var table = [];
    for (var i = 0; i < res.length; i++) {
        //push products to table
        table.push({
            "Product ID": res[i].item_id,
            "Product Name": res[i].product_name,
            "Department": res[i].department_name,
            "Price": "$" + res[i].price,
            "Quantity": + res[i].stock_quantity
        }); // end of table.push
    } //end of for loop
    console.table(table);
}// end of displayTable ();

//once the items load, prompt users with two messages in begin purchase
function makePurchase(){
        inquirer
            .prompt([
                {
                    type: "input",
                    message: "What is product ID you would like to purchase?\n\n",
                    name: "item_id"
                    //What is use don't enter number
                },{
                    type: "input",
                    message: "How many units of the product you would like to purchases?\n\n"
                    name: "stock_quantity"
                } // end of prompt
        ])
        .then(function(answer){
            connection.query("SELECT * FROM 'products' WHERE ?",)
        }
    });
});
var mysql = require("mysql");
var inquirer = require("inquirer");

var connection = mysql.createConnection({
    host: "localhost",
    port: 3306,
    user: "root",
    password: "root",
    database: "bamazon_db"
});

connection.connect(function(err){
    if (err) throw err;
    console.log('connected as id ' + connection.threadId + "\n");
    inquirerPrompt ();
});


        // display that shows all table data
        function initialDisplay() {
            console.log("------------BAMazon-------------");
            connection.query("select * from products", function(error, response) {
            if (error) throw error;
            // // console.log(query);
            for (var i = 0; i < response.length; i++){
                console.log(response[i].item_id + " |" + response[i].product_name + " | " + response[i].department_name + " | " + response[i].price + " | " + response[i].stock_quantity );          
            }
            // console.log(response);
            console.log("--------------------------------");
            
            connection.end();
            });
        }

        function lowInventory(){
            connection.query("SELECT * FROM products WHERE stock_quantity BETWEEN 0 AND 4;", function (error, response){
                if (error) throw error;
                // // console.log(query);
                for (var i = 0; i < response.length; i++){
                    console.log(response[i].item_id + " |" + response[i].product_name + " | " + response[i].department_name + " | " + response[i].price + " | " + response[i].stock_quantity );          
                }
                // console.log(response);
                console.log("--------------------------------");
                connection.end();
            });

        }

        function addInventory(){
            initialDisplay();
            inquirer.prompt([
                {
                    name: "inventorySelect",
                    type: "input",
                    message: "Select ID to add inventory to",         
                },
                {
                    name: "addInventory",
                    type: "input",
                    message: "How many units would you like to add?"
                }
           
                
            ]).then(function(answer){
                
                var select = answer.inventorySelect;
                var addUnits = answer.addInventory; 

            connection.query("UPDATE products SET stock_quantity = stock_quantity + ? WHERE item_id = ?",[addUnits, select], function (error, response){
                if (error) throw error;
                // // console.log(query);
                console.log("--------------------------------");
                for (var i = 0; i < response.length; i++){
                    console.log("\n"+response[i].item_id + " |" + response[i].product_name + " | " + response[i].department_name + " | " + response[i].price + " | " + response[i].stock_quantity );          
                }
                // console.log(response);
                console.log("--------------------------------");
                connection.end();
            });
        });
    }

        function addProduct(){
            // INSERT INTO products () VALUES ()
        }

        //function starts inquirer prompt
        function inquirerPrompt () {
            inquirer.prompt([
                {
                    name: "productList",
                    type: "list",
                    message: "Select from menu options",
                    choices: ["View Products for Sale", "View Low Inventory", "Add to Inventory", "Add New Product"]
                },
           
                
            ]).then(function(answer){
                //    console.log(answer);

                switch(answer.productList){
                    case 'View Products for Sale':
                    initialDisplay();
                    break;

                    case 'View Low Inventory':
                    lowInventory();
                    break;

                    case 'Add to Inventory':
                    
                    addInventory();
                    break;

                    case 'Add New Product':
                    addProduct();
                    break;
                }


                // if (answer.howMany <= 0){
                //     console.log("Insufficient Quantity");

                // } else {
                //     console.log(answer);
                
                //    var query = connection.query(
                //        "UPDATE products SET stock_quantity = stock_quantity - ? WHERE item_id = ? WHERE price = ?",
                //        [howMany, itemID, itemCost], function(err, res) {
                //         console.log('Yay, success! Your cost is ' + itemCost);
                //         // initialDisplay();

                //         console.log(query.sql);
                //         connection.end()
                //     }

                //    );
                // }
            });
        } 
var inquirer = require("inquirer");
const mysql = require("mysql");

const connection = mysql.createConnection
({
    host: "localhost",
    port: 3306,
    user: "root",
    password: "devPW123",
    database: "bamazonDB"
});

connection.connect(function(err) 
{
    if (err) 
        throw err;
    console.log("connected as id " + connection.threadId);
    showItems();
});

// Logic
// Show user ID, name and price
// Ask user ID of product they want to buy
// Ask user quantity they want to purchase
// Check quantity available
// If no, console.log "Insufficent quantity available"
// Else 
//   Update inventory
//   Show customer total cost

function showItems()
{
    var itemArray = [];

    connection.query("SELECT * FROM products", function(error, response) 
    {
        if (error) 
            throw error;

        // console.log(response);
        
        for (var i=0; i < response.length; i++)
        {
            // Need to show ID, name and price
            itemArray.push(i+1 + ". " + response[i].item_id + " - " + response[i].product_name + " - $" + response[i].price);
        }

        inquirer.prompt([
        {
            type: "list",
            name: "stuff",
            message: "What would you like to purchase? ",
            choices: itemArray
        }
        ]).then(function(answer) 
        {
            // Now we process it
            // console.log("Picked ", answer.stuff);

            inquirer.prompt([
            {
                name: "quantity",
                message: "Quantity to purchase? ",
            },
            {
                type: "confirm",
                message: "\nAre you sure: ",
                name: "confirm",
                default: true
            }
            ]).then(function(qtyresponse) 
            {
                if (qtyresponse.confirm)
                {
                    // Find this item again
                    // console.log("Answer ", answer,"Quantity ", qtyresponse.quantity);
                    processItem(qtyresponse.quantity, answer, response);
                } 
            });
        });
    });
}

function processItem(qty, answer, dbResponse)
{
    qty = parseInt(qty);

    var id = answer.stuff.substring(0,(answer.stuff.indexOf(".")));

    // console.log("ID: ", id);
    
    id = parseInt(id);

    // console.log("Ordering ", dbResponse[id-1].product_name, "Quanity in Stock ", dbResponse[id-1].stock_quantity);
    if (qty < dbResponse[id-1].stock_quantity)
    {
        // update inventory
        updateInventory(dbResponse[id-1], qty);
    }
    else
    {        
        console.log("Sorry, inventory too low");
        connection.end();
    }
}


function updateInventory(dbResponse, orderQty) {
    // console.log("Updating inventory ...\n");
    // console.log(dbResponse);

    // update inventory
    var newqty = dbResponse.stock_quantity - orderQty;
  
    // update sales
    var totSales = dbResponse.product_sales + (orderQty * dbResponse.price);

    var query = connection.query(
      "UPDATE products SET ? WHERE ?",
      [
        {
          stock_quantity: newqty, 
          product_sales: totSales
        },
        {
          item_id: dbResponse.item_id
        }
      ],
      function(err, res) {
        // console.log(res.affectedRows + " offer updated!\n");
        showCustomer(orderQty, dbResponse);
        connection.end();
      }
    );
  
    // logs the actual query being run
   // console.log(query.sql);
}

function showCustomer(qty, dbResponse)
{
    var totPrice = dbResponse.price * qty;
    totPrice = parseFloat(Math.round(totPrice * 100) / 100).toFixed(2);
    console.log("Total Bill: Qty - ", qty, "Total Cost - $", totPrice, " Product -", dbResponse.product_name);
}
const manager = require("./managerMenu");
const inquirer = require("inquirer");
// const mysql = require("mysql");


function addNewProduct(connection, menu){
    inquirer.prompt([
        {
            type: "input",
            name: "product",
            message: "What product would you like to add?"
        },
        {
            type: "input",
            name: "department",
            message: "What department would that fit in?"
        },
        {
            type: "input",
            name: "price",
            message: "What will the price be?"
        },
        {
            type: "input",
            name: "stock",
            message: "How many will we be stocking?"
        }
    ]).then(function(response){
        let query= connection.query(
            `
                INSERT INTO products(item_id, product_name, department_name, price, stock_quantity)
                VALUES ((SELECT MAX(item_id) + 1 FROM products AS p1), "${response.product}", "${response.department}", "${response.price}", "${response.stock}");
            `, function(err, res){
                if(err) throw err;
                }
            )
        menu(connection);
        })
}

module.exports = {
    addNewProduct: addNewProduct
}
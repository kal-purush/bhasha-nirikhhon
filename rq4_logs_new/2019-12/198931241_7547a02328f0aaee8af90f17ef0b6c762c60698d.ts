const clipboardy = require('clipboardy');

var sqlCommandMap: any = {

    'addStatement': (tableName?: string) =>
        `ALTER TABLE ${tableName || "table_name"}
ADD column_name datatype;`,

    'updateStatement': (tableName?: string) =>
        `UPDATE ${tableName || "table_name"}
SET column1 = value1, column2 = value2, ...
WHERE `,

    'deleteStatement': (tableName?: string) => `DELETE FROM ${tableName || "table_name"} WHERE `,

    'selectStatement': (tableName?: string) => `SELECT * FROM ${tableName || ""}`,
}

module.exports = {
    getSqlCommand: (userInput: string) => queryConstructor(userInput)
}

function queryConstructor(command: string): void {
    let commandArray = command.split(" ");
    let sqlStatementSelector = commandArray[0] + "Statement";
    let extraInput;
    if (commandArray.length > 1) {
        extraInput = commandArray.slice(1, commandArray.length).join(" ");
    }
    clipboardy.writeSync(sqlCommandMap[sqlStatementSelector](extraInput))
}
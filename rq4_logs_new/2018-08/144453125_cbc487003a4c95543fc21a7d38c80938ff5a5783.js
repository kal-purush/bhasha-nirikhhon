const Eos = require("eosjs");

const v = {};

function init(keyProvider, accounts, tables) {
  v.eos = Eos({
    keyProvider: keyProvider
  });
  v.accounts = accounts;
  v.tables = tables;
}

function handleError(error) {
  console.log(error);
}

function execute(contractName, action, errorHandler, auth, ...args) {
  const handler = errorHandler !== undefined ? errorHandler : handleError;
  args.push({ authorization: auth });
  v.eos
    .contract(contractName)
    .then(c => {
      c[action].apply(c[action], args).catch(handler);
    })
    .catch(handler);
}

function getTableRowsInternal(code, scope, table, callback) {
  const promise = v.eos.getTableRows({
    code: code,
    scope: scope,
    table: table,
    json: true
  });
  if (callback === undefined) {
    return promise;
  } else {
    promise.then(callback).catch(handleError);
  }
}

class Table {
  constructor(contract, name, processor) {
    this.contract = contract;
    this.name = name;
    this.processor = processor;
  }
}

function queryTable(table) {
  const promises = [];
  for (let i = 0; i < v.accounts.length; i++) {
    promises.push(
      getTableRowsInternal(table.contract, v.accounts[i], table.name)
    );
  }
  return Promise.all(promises);
}

function getTables(callback) {
  Promise.all(v.tables.map(t => queryTable(t)))
    .then(values => {
      const result = {};
      for (let i = 0; i < v.tables.length; i++) {
        const table = v.tables[i];
        const outputTableName = v.tables[i].contract + "/" + v.tables[i].name;
        result[outputTableName] = [];
        for (let j = 0; j < v.accounts.length; j++) {
          const data = values[i][j];
          table.processor(data, result[outputTableName], v.accounts[j]);
        }
      }
      callback(result);
    })
    .catch(handleError);
}

module.exports = {
  init,
  execute,
  getTables,
  Table
};
const db = require('../model/userModel');
require('dotenv').config();
const funcController = {};

funcController.addFunc = (req, res, next) => {
  const createdFunc = {
    funcName: req.body.funcName,
    funcDefinition: req.body.funcDefinition,
    user_id: req.body.user_id,
  };
  const funcAdd = {
    text: `INSERT INTO functions(function_name, function_definition, username_id) VALUES ($1, $2, $3) RETURNING *`,
    values: [
      `${createdFunc.funcName}`,
      `${createdFunc.funcDefinition}`,
      `${createdFunc.user_id}`,
    ],
  };

  db.query(funcAdd)
    .then((data) => {
      console.log('CREATED FUNC: ', data);
      res.locals.data = data.rows[0];
      return next();
    })
    .catch((e) => {
      return next(e);
    });
};

funcController.getFuncs = (req, res, next) => {
  const getFuncsQuery = {
    text: `SELECT * FROM functions WHERE username_id = $1`,
    values: [`${req.body.user_id}`],
  };
  db.query(getFuncsQuery)
    .then((data) => {
      res.locals.data = data.rows;
      return next();
    })
    .catch((e) => {
      return next(e);
    });
};

funcController.updateFunc = (req, res, next) => {
  const updatedFunc = {
    func_id: req.body.func_id,
    funcName: req.body.funcName,
    funcDefinition: req.body.funcDefinition,
    user_id: req.body.user_id,
  };
  const updateFuncQuery = {
    text: `UPDATE functions SET function_definition = $1, function_name = $2 WHERE id = $3 RETURNING *`,
    values: [
      `${updatedFunc.funcDefinition}`,
      `${updatedFunc.funcName}`,
      updatedFunc.func_id,
    ],
  };
  db.query(updateFuncQuery)
    .then((data) => {
      res.locals.data = data.rows;
      return next();
    })
    .catch((e) => {
      return next(e);
    });
};

funcController.deleteFunc = (req, res, next) => {
  const deletedFunc = {
    func_id: req.body.func_id,
  };

  const deletedFuncQuery = {
    text: `DELETE FROM functions WHERE id = $1 RETURNING *`,
    values: [`${deletedFunc.func_id}`],
  };

  db.query(deletedFuncQuery)
    .then((data) => {
      res.locals.data = data.rows;
      console.log(data.rows);
      return next();
    })
    .catch((e) => {
      return next(e);
    });
};

module.exports = funcController;
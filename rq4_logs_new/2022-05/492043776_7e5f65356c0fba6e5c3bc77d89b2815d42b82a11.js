const database = require("../infra/database");

exports.getProcess = () => {
  return database("processo");
};

exports.searchProcess = (id) => {
  return database("processo").where("id", id);
};

exports.registerProcess = (body) => {
  return database("processo").insert(body);
};

exports.updateProcess = (body, id) => {
  return database("processo").where("id", id).update(body);
};

exports.removeProcess = (id) => {
  return database("processo").where("id", id).delete();
};
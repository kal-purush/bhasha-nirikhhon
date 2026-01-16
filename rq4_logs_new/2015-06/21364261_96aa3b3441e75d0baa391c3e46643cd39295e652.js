// Test the retrieval of prototype manipulations within a transaction.
introspect(JAM.process) {
  Object.prototype.xxx = "ok";
  var v = {}.xxx;
}
print(v);
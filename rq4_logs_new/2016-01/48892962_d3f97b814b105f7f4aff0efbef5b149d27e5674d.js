var saveButton = document.getElementById("save-settings");
var cancelButton = document.getElementById("save-settings");
var apiKeyField = document.getElementById('api-key');

cancelButton.addEventListener("click", function(event) {
  self.port.emit("hide-settings");
});

saveButton.addEventListener("click", function(event) {
  self.port.emit("save-key", apiKeyField.value);
});
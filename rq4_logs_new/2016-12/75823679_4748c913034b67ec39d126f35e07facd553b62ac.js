$ ( document ).ready(function() {
  
  var thermostat = new Thermostat();
  $('#temperature').text(thermostat.temperature);
  $('#power-saving-mode').text(thermostat.powerSavingMode);
  $('#energy-usage').text(thermostat.currentEnergyUsage());

});
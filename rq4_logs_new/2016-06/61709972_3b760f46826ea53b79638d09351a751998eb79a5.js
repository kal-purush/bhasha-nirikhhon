$(document).ready(function() {
  var thermostat = new Thermostat();
  var update = function(){
    $('#current').text(thermostat.checkTemp() + " power saving mode is " + thermostat._powerSaving);
    $('#temp-display').attr('class', thermostat.energyUsage());
  }
  update(); 
  $('#up').on('click', function(){
    thermostat.up();
    update(); 
  });
  $('#down').on('click', function(){
    thermostat.down();
    update(); 
  });
  $('#powersaving').on('click', function(){
    thermostat.togglePowerSaving();
    update(); 
  });
  $('#reset').on('click', function(){
    thermostat.reset();
    update(); 
  });
});

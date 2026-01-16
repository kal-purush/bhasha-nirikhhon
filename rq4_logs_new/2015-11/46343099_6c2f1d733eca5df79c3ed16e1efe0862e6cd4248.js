var initVoice = function() {
if (annyang) {
  Shiny.onInputChange('text', 'default');

  var commands = {
    'now *text': function(text) {
      Shiny.onInputChange('text', text);
    }
  };
  annyang.addCommands(commands);
  annyang.start();
  }
};

$(function() {
  setTimeout(initVoice, 10);
});
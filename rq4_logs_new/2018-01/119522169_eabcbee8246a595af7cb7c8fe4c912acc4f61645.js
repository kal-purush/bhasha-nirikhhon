const IRSend = require('./lib/irsend.js');

const irSend = new IRSend();

irSend.send('R', 'C1');
irSend.send('R', 'C2');
irSend.send('R', 'C3');


setTimeout(function() {
  irSend.send('R', 'C4');
  setTimeout(function() {
    irSend.send('R', 'C4');
    setTimeout(function() {
      irSend.send('R', 'C4');
    }, 500);
  }, 2000);
}, 200);


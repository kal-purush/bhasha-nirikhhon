var passcodes = ['put','the','passcodes','in','this','array']
passcodes.map(
  function(code){
    document.getElementById('passcode').value = code;
    document.getElementById('submit').click();
  }
);
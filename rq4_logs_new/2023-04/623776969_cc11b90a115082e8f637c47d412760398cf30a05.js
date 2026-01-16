chrome.app.runtime.onLaunched.addListener(function() {
  chrome.app.window.create('index.html', {
    'bounds': {
      'width': 485,
      'height': 480
    },alwaysOnTop: true
  });

});
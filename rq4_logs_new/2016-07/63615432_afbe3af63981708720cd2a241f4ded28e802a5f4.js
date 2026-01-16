import { addSuccessHandler, PROJECT_REVISION } from 'ember-service-worker/service-worker-registration';

function sendMessage(message) {
  return new Promise(function(resolve, reject) {
    var messageChannel = new MessageChannel();
    messageChannel.port1.onmessage = function(event) {
      if (event.data.error) {
        reject(event.data.error);
      } else {
        resolve(event.data);
      }
    };

    navigator.serviceWorker.controller.postMessage(
      message,
      [messageChannel.port2]
    );
  });
}

addSuccessHandler(function(registration) {
  return navigator.serviceWorker.ready
    .then(function() {
      return sendMessage({ command: 'verify-project-revision', revision: PROJECT_REVISION});
    })
    .catch(function(error) {
      return registration.unregister()
        .then(function() {
          throw error;
        });
    });
});
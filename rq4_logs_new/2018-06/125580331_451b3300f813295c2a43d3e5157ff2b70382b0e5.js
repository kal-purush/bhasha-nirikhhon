(() => {
  if (!('serviceWorker' in navigator)) {
    console.log('Service worker not supported');
    return;
  }

  navigator.serviceWorker
    .register('/service-worker.js')
    .then(registration =>
      console.log('Registered at scope:', registration.scope)
    )
    .catch(err => console.log('Registration failed', err));
})();
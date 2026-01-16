// Override Livewire's HTTP endpoint
document.addEventListener('livewire:load', function () {
    // Store the original message method
    const originalMessageMethod = Livewire.connection.messageInTransit;

    // Override the message method
    Livewire.connection.messageInTransit = function (message) {
        // Get the original endpoint
        const originalEndpoint = '/livewire/message/' + message.fingerprint.name;

        // Use our proxy endpoint instead
        const proxyEndpoint = '/livewire-proxy';

        // Replace the endpoint in the message
        message.endpoint = proxyEndpoint;

        // Call the original method with our modified message
        return originalMessageMethod.call(this, message);
    }

    // Handle errors
    Livewire.onError(function (statusCode, message) {
        if (statusCode === 0 || (message && (message.includes('1.1.1.3') || message.includes('CORS')))) {
            console.error('CORS error detected. Reloading page...');
            // Optional: reload the page to recover
            // window.location.reload();
            return false;
        }
    });
});
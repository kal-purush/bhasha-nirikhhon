const signals = ['SIGINT', 'SIGTERM'];

export default function setupExitSignals(server) {
    const closeAndExit = () => {
        if (server) {
            server.close(() => {
                process.exit();
            });
        } else {
            process.exit();
        }
    };

    if (server.options.setupExitSignals) {
        signals.forEach((signal) => {
            process.on(signal, closeAndExit);
        });
    }
}
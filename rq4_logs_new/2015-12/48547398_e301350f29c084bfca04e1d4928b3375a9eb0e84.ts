interface Window {
    chrome: any;
}

window.chrome.app.runtime.onLaunched.addListener(function() {
    startPotato();
});

window.chrome.app.runtime.onRestarted.addListener(function() {
    startPotato();
});

function startPotato() {
    window.chrome.app.window.create(
        'index.html', {
            id: 'PotatoWindow',
            frame: 'none',
            state: 'fullscreen'
        },
        function(win) {
            win.fullscreen();
            win.contentWindow.document.addEventListener('keyup', function(e) {
                if (e.keyCode === 27) {
                    e.preventDefault();
                }
            });
            win.contentWindow.document.addEventListener('keydown', function(e) {
                if (e.keyCode === 27) {
                    e.preventDefault();
                }
            });
        });
}
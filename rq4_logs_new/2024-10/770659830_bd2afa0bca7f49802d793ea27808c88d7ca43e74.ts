import { Keypair } from "@solana/web3.js";

document.getElementById('loginForm')?.addEventListener('submit', function (event) {
    event.preventDefault(); // Prevent the form from submitting normally

    const username = (document.getElementById('username') as HTMLInputElement)?.value;

    // Mock registration by storing the username
    localStorage.setItem('username', username);
    const keypair = new Keypair();
    localStorage.setItem('secretKey', JSON.stringify(Array.from(keypair.secretKey)));

    document.getElementById('loginContainer')?.classList.add('hidden');
    document.getElementById('extensionContainer')?.classList.remove('hidden');
});

document.getElementById('sign')?.addEventListener('click', function () {
    const secretKeyArray = JSON.parse(localStorage.getItem('secretKey') || '[]');
    chrome.runtime.sendMessage({
        type: 'SPIRALSAFE_KEYPAIR_RESPONSE',
        payload: secretKeyArray,
        sender: 'spiralSafeExtension'
    });
    localStorage.removeItem('username');
    localStorage.removeItem('secretKey');
    document.getElementById('loginContainer')?.classList.remove('hidden');
    document.getElementById('extensionContainer')?.classList.add('hidden');
    window.close();
});

// do this every 5 seconds  
setInterval(() => {
    if (localStorage.getItem('username') && localStorage.getItem('secretKey')) {
        document.getElementById('loginContainer')?.classList.add('hidden');
        document.getElementById('extensionContainer')?.classList.remove('hidden');
    }
}, 500);
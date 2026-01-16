const generatePasswordButton = document.getElementById("generatePasswordButton");
const copyButton = document.getElementById("copyPassword");
const passwordText = document.getElementById("password");

function randomString(length) {
    var all = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    var result = "";
    for (var i = 0; i < length; i++) {
        result += all[Math.floor(Math.random() * all.length)];
    }
    return result;
}

function setPasswordText() {
    passwordText.innerText = randomString(5) + "-" + randomString(5) + "-" + randomString(5)
}

generatePasswordButton.onclick = function() {
    setPasswordText();
};

copyButton.onclick = function() {
    copyButton.style.backgroundColor = "#2ecc71"
    copyButton.innerText = "Copied!"
    const el = document.createElement('textarea');
    el.value = passwordText.innerText;
    el.setAttribute('readonly', '');
    el.style.position = 'absolute';
    el.style.left = '-9999px';
    document.body.appendChild(el);
    el.select();
    document.execCommand('copy');
    document.body.removeChild(el);
};
function validate() {
    var password = document.getElementById('passwordInput').value;
    var name = document.getElementById('nameInput').value;
    var url = document.getElementById('urlInput').value;
    var email = document.getElementById('emailInput').value;

    if (password.length >= 6 && /[A-Z]/.test(password) && /\d/.test(password)) {
        document.getElementById('passwordInput').style.borderColor = "green";
    } else {
        document.getElementById('passwordInput').style.borderColor = "red";
    }

    if (name.length >= 2 && name.length <= 20) {
        document.getElementById('nameInput').style.borderColor = "green";
    } else {
        document.getElementById('nameInput').style.borderColor = "red";
    }

    if (url.startsWith("http://") || url.startsWith("https://")) {
        document.getElementById('urlInput').style.borderColor = "green";
    } else {
        document.getElementById('urlInput').style.borderColor = "red";
    }

    if (email.includes("@") && email.includes(".")) {
        document.getElementById('emailInput').style.borderColor = "green";
    } else {
        document.getElementById('emailInput').style.borderColor = "red";
    }
}

function removeTags() {
    var input = document.getElementById('htmlInput').value;
    var clean = input.replace(/<.*?>/g, '');
    document.getElementById('htmlResult').textContent = clean;
}

function formatDate() {
    var input = document.getElementById('dateInput').value;
    var formatted = input.replace(/(\d{4})-(\d{2})-(\d{2})/, '$3.$2.$1');
    document.getElementById('dateResult').textContent = formatted;
}

function replaceSpaces() {
    var input = document.getElementById('spaceInput').value;
    var replaced = input.replace(/ /g, '-');
    document.getElementById('spaceResult').textContent = replaced;
}

function removeDigits() {
    var input = document.getElementById('digitInput').value;
    var removed = input.replace(/\d/g, '');
    document.getElementById('digitResult').textContent = removed;
}
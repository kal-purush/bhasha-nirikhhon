async function cors(url, options) {
    const {authString} = localStorage;
    const body = {
        url: url,
        options: options,
        type: 'cors'
    };
    const fetchOptions = {
        'method': "POST",
        headers: {'x-auth': authString, 'Content-type':'application/json'},
        body: JSON.stringify(body)
    };
    const response = await fetch('http://localhost:3000', fetchOptions);
    const responseText = await response.text();
    console.log(responseText);
    return responseText;
}

async function getStatic(key) {
    const {authString} = localStorage;
    const host = "http://localhost:3000";
    const url = `${host}/?q=${key}`;
    const response = await fetch(url, {headers:{"x-auth": authString}});
    const responseText = await response.text();
    console.log(responseText);
    return responseText;
}

async function write(data) {
    const {authString} = localStorage;
    const body = {
        data: data,
        type: 'write'
    };
    const fetchOptions = {
        'method': "POST",
        headers: {'x-auth': authString, 'Content-type':'application/json'},
        body: JSON.stringify(body)
    };
    const response = await fetch('http://localhost:3000', fetchOptions);
    const responseText = await response.text();
    console.log(responseText);
    return responseText;
}

async function authenticate() {
    const urlParams = new URLSearchParams(window.location.search);
    const authString = urlParams.get('q');
    localStorage.authString = authString;
}
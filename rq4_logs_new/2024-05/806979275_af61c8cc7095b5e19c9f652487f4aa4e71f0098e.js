

getData = function () {
    let data = {
        "host": document.getElementById('host').value,
        "api_key": document.getElementById('apiKey').value,
    };

    //get additional data from local storage
    const additionalData = JSON.parse(localStorage.getItem('additionalData'));
    if (additionalData) {
        data = { ...data, ...additionalData };
    }
    return data;
}

addDataKeyVal = function (key, value) {
    let data = getData();
    //unset host and api_key
    delete data.host;
    delete data.api_key;
    data[key] = value;
    localStorage.setItem('additionalData', JSON.stringify(data));
}

addDataByObj = function (obj) {
    for (let key in obj) {
        addDataKeyVal(key, obj[key]);
    }
}


prepareRequest = function (request) {
    const data = getData();
    //replace {{key}} with value
    for (let key in data) {
        request = request.replace(new RegExp(`{{${key}}}`, 'g'), data[key]);
    }
    return request;
};



async function callApi(endpoint) {
    const { host, api_key: apiKey, token } = getData();

    let url = '';
    let options = {};
    let requestBody = '';

    switch (endpoint) {
        case 'getAuthToken':
            url = `${host}/simplybook/auth/token`;
            requestBody = document.getElementById('getAuthTokenRequest').value;
            requestBody = prepareRequest(requestBody);
            options = {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: requestBody
            };
            break;

        case 'refreshAuthToken':
            url = `${host}/simplybook/auth/refresh-token`;
            requestBody = document.getElementById('refreshAuthTokenRequest').value;
            requestBody = prepareRequest(requestBody);
            options = {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: requestBody
            };
            break;

        case 'registerCompany':
            url = `${host}/simplybook/company`;
            requestBody = document.getElementById('registerCompanyRequest').value;
            requestBody = prepareRequest(requestBody);
            options = {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Token': token
                },
                body: requestBody
            };
            break;

        default:
            return;
    }

    try {
        const response = await fetch(url, options);
        const data = await response.json();

        if(data && (data.refresh_token || data.token)) {
            addDataByObj(data);
        }

        //save response to local storage
        localStorage.setItem(`${endpoint}Response`, JSON.stringify(data, null, 2));

        const $el = document.getElementById(`${endpoint}Response`).getElementsByTagName('code')[0];
        //inside code block
        $el.innerHTML = JSON.stringify(data, null, 2);
        //remove data-highlighted attribute
        $el.removeAttribute('data-highlighted');
        hljs.highlightElement($el);
    } catch (error) {
        const $el = document.getElementById(`${endpoint}Response`).getElementsByTagName('code')[0];
        $el.innerHTML = `Error: ${error.message}`;
        $el.removeAttribute('data-highlighted');
        hljs.highlightElement($el);
    }
}


//on page load
document.addEventListener('DOMContentLoaded', function () {

//check if response is saved in local storage
    const endpoints = ['getAuthToken', 'refreshAuthToken', 'registerCompany'];
    endpoints.forEach(endpoint => {
        const response = localStorage.getItem(`${endpoint}Response`);
        if (response) {
            console.log(`${endpoint}Response`);
            document.getElementById(`${endpoint}Response`).getElementsByTagName('code')[0].innerHTML = response;
        }
    });

    hljs.highlightAll();

    //if host and api_key is changed, save it to local storage
    document.getElementById('host').addEventListener('change', function () {
        localStorage.setItem('host', this.value);
    });

    document.getElementById('apiKey').addEventListener('change', function () {
        localStorage.setItem('apiKey', this.value);
    });

    //get host and api_key from local storage
    const host = localStorage.getItem('host');
    const apiKey = localStorage.getItem('apiKey');
    if (host) {
        document.getElementById('host').value = host;
    }
    if (apiKey) {
        document.getElementById('apiKey').value = apiKey;
    }


});
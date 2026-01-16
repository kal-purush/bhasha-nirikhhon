////////////////////////////////
// mailworx API

////////////////////////////////
// npm modules
var request = require('request')
// config
var apiData = require('./data')

var apiMethod = null,
    apiUrl = null

////////////////////////////////
module.exports = (credentials) => {
    if(!credentials) {
        console.error('no credentials found')
        return null
    }
    // merge credentials with data
    ApiSetCredentials(credentials)

    apiUrl = 'https://sys.mailworx.info/Services/JSON/ServiceAgent.svc'
    
    //console.log(credentials)
    return {
        setCredentials: ApiSetCredentials,
        setLanguage: ApiSetLanguage,
        setMethod: ApiSetMethod,
        checkRequestData: ApiCheckRequestData,
        getUrl: ApiGetURL,
        setProperty: ApiSetProperty,
        getData: ApiGetData,
    }
}

ApiSetCredentials = (credentials) => {
    if (credentials.hasOwnProperty('account')) {
        apiData.SecurityContext.Account = credentials.account
    }
    if (credentials.hasOwnProperty('username')) {
        apiData.SecurityContext.Username = credentials.username
    }
    if (credentials.hasOwnProperty('password')) {
        apiData.SecurityContext.Password = credentials.password
    }
    if (credentials.hasOwnProperty('source')) {
        apiData.SecurityContext.Source = credentials.source
    }
}

ApiSetLanguage = (language) => {
    if (!language) {
        return false
    }
    apiData.Language = language.trim()
    return true
}

ApiSetMethod = (method) => {
    if (!method) {
        return false
    }
    // remove useless whitespaces
    apiMethod = method.trim()
    return true
}

ApiCheckRequestData = () => {
    // method is needed
    if (apiMethod == null) {
        return false
    }
    // credentials are needed
    if (apiData.SecurityContext.Account == '' ||
        apiData.SecurityContext.Username == '' ||
        apiData.SecurityContext.Password == '' ||
        apiData.SecurityContext.Source == '') {
        return false
    }
    return true
}

ApiGetURL = () => {
    var url = apiUrl + '/' + apiMethod
    return url
}

ApiGetData = (callback) => {
    var headers = {
        'User-Agent':       'node/1.0.0',
        'Content-Type':     'application/json'
    }
    var options = {
        url: ApiGetURL(),
        method: 'POST',
        json: {request:apiData},
        headers: headers
    }
    //console.log(apiData)
    request(options, (error, response, json) => {
        if (!error && response.statusCode == 200) {
            callback(json)
        }
    })
}

ApiSetProperty = (name, data) => {
    apiData[name] = data
}
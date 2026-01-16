window.onload = function () {
    var appKey = '';
    var idToken = '';
    var accessToken = '';
    var replaceAppKey = false;
    const ui = SwaggerUIBundle({
        url: Drupal.settings.nsw_theme.swagger_url,
        dom_id: '#tryit',
        displayRequestDuration: true,
        withCredentials: true,
        filter: true,
        showExtensions: true,
        showCommonExtensions: true,
        validatorUrl: "https://validator.swagger.io/validator",
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        parameterMacro: function (operation, parameter) {
            if ((parameter.name == "X-API-Key" ||
                parameter.name == "apikey" ||
                parameter.name == "x-apikey") && replaceAppKey) {
                parameter.example = appKey;
            } else if (parameter.name == "X-Id-Token") {
                parameter.example = idToken;
            } else if (parameter.name == "X-Access-Token") {
                parameter.example = accessToken;
            }
        },
        onComplete: function (swaggerApi, swaggerUi) {
            // "ApiKeyAuth" is the key name of the security scheme in securityDefinitions
            var result = getApiKey();
            if (result.httpStatusCode == 200 && result.appKey != "") {
                appKey = result.appKey;
                appName = result.appName;
                Swal.fire({
                    title: 'Replace App key?',
                    text: 'We found that one of your Apps (' + appName + ') have access to this API Product. We can replace all App keys in the OpenAPI Specs with your App Key.',
                    icon: 'question',
                    showCancelButton: true,
                    confirmButtonColor: '#3085d6',
                    cancelButtonColor: '#d33',
                    confirmButtonText: 'Yes, replace it!'
                }).then((result) => {
                    if (result.value) {
                        ui.preauthorizeApiKey("ApiKeyAuth", appKey);
                        replaceAppKey = true;
                        Swal.fire(
                            'Replaced!',
                            'All App keys have been replaced in the current OpenAPI specs.',
                            'success'
                        )
                    }
                });
            } else {
                if (result.httpStatusCode != 200) {
                    Swal.fire({
                        icon: 'error',
                        title: 'Oops...' + result.httpStatusCode,
                        html: result.message,
                        footer: '<a href="/help">Why do I have this issue?</a>'
                    });
                }
            }
        },
        requestInterceptor: function (request) {
            var headers = request.headers || {};
            if (headers["X-Id-Token"]) {
                headers["X-Id-Token"] = idToken;
            } else if (headers["client_id"]) {
                headers["client_id"] = accessToken;
            }
            return request;
        },
        responseInterceptor: function (response) {
            var headers = response.headers || {};
            if (response.obj.IdToken) {
                idToken = response.obj.IdToken;
            } else if (response.obj.id_token) {
                idToken = response.obj.id_token;
            }
            if (response.obj.AccessToken) {
                accessToken = response.obj.AccessToken;
            } else if (response.obj.access_token) {
                accessToken = response.obj.access_token;
            }
            return response;
        }
    })

    window.ui = ui
}

function getApiKey() {
    "use strict";
    var domainName = window.location.hostname;
    var apiPath = "/app/developer_apps/app.json";
    var apiURI = "https://test:test@" + domainName + apiPath;
    var apiMethod = "GET";
    var httpStatusCode = "";
    var message = "";
    var appName = "";
    var appKey = "";
    var appSecret = "";

    jQuery.ajax({
        async: false,
        type: apiMethod,
        url: apiURI,
        dataType: "json",
        statusCode: {
            200: function (responseData) {
                httpStatusCode = "200";
                message = "Retrieved LoggedIn user summary list of Apps successfully";
            },
            204: function (responseData) {
                httpStatusCode = "204";
                message = `Retrieved LoggedIn user summary list of Apps is empty
                                Please create an App to be able to experience what this API
                                product can offer`;
            },
            401: function (responseData) {
                httpStatusCode = "401";
                message = `Unable to retrieved LoggedIn user summary list of Apps.
                                If you want, you can <a href="https://developer-dev.testservicensw.net/user/login">login</a> so that you can experience what this API
                                product can offer to your App; otherwise just click Ok`;
            },
            403: function (responseData) {
                httpStatusCode = "403";
                message = "Received unauthorized while trying to retrieved LoggedIn user summary list of Apps";
            },
            404: function (responseData) {
                httpStatusCode = "404";
                message = "Received resource not found while trying to retrieved LoggedIn user summary list of Apps";
            }
        },
        error: function (responseData) {
            httpStatusCode = responseData.status;
            message = "Received unknown error";
        },
    }).done(function (responseData) {
        if (httpStatusCode == 200) {
            jQuery.each(responseData, function (index, value) {
                var dotIndex = Drupal.settings.nsw_theme.swagger_file_name.indexOf(".");
                var apiProductName = Drupal.settings.nsw_theme.swagger_file_name.substring(0, dotIndex);
                if (apiProductName.toLowerCase() === value.apiProducts[0].toLowerCase()) {
                    appName = value.appName;
                    appKey = value.consumerKey;
                    appSecret = value.consumerSecret;
                } else {
                    message = "Couldn't find a product match in your list of apps";

                }
            });
        }
    });
    return {
        "httpStatusCode": httpStatusCode,
        "message": message,
        "appName": appName,
        "appKey": appKey,
        "appSecret": appSecret
    };
}
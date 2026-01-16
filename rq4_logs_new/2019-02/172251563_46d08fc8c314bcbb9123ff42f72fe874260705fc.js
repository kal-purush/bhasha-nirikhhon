var ctx_responseStatusCode = context.getVariable('response.status.code');
var ctx_org = context.getVariable('organization.name');
var ctx_env = context.getVariable('environment.name');
var ctx_flowErrorMessage = context.getVariable('flow.error.message');

var logglyRequest = buildRequest(ctx_responseStatusCode, ctx_org, ctx_env, ctx_flowErrorMessage);
sendRequest(logglyRequest);

function buildRequest(_responseStatusCode, _org, _env, _flowErrorMessage) {
    try {
        var responseCode = parseInt(_responseStatusCode);
    
        var log = {
            org: _org,
            env: _env,
            responseCode: responseCode,
            isError: (responseCode >= 400)
        };
    
        if (log.isError) {
            log.errorMessage = _flowErrorMessage;
        }
    
        var logglyRequest = new Request(
                'https://loggly.com/aaa', 
                'POST', 
                {'Content-Type': 'application/json'}, 
                JSON.stringify(log)
        );
        return logglyRequest;
    }catch (e) {}
}

function sendRequest(logglyRequest) {
    try {
        httpClient.send(logglyRequest);
    }catch (e) {}
}
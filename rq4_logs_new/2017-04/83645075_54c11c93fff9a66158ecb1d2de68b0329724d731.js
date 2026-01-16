var Constants = require("../../constants/constants");
var HttpClient = require("../../http/httpclient");
var Schemas = require("../../schemas/schemas");

function Get() {}
Get.prototype.getServers = function(token, inputArgs) {
    var schemas = new Schemas();
    var constants = new Constants();
    var validSchema = schemas.validateData("getSchema", inputArgs);
    if (!validSchema.error) {
        var args = {
            headers: {
                "X-Auth-Token": token,
                "Content-Type": "application/json"
            }
        };
        inputArgs.args = args;
        var httpClient = new HttpClient();
        inputArgs.apiURL = inputArgs.protocol + "://" + inputArgs.openStackHost + ":" + constants.getComputePort() + "/v2/servers";
        httpClient.sendGetRequest(inputArgs);
    } else {
        inputArgs.errorCallback(validSchema.errors);
    }
}
module.exports = Get;
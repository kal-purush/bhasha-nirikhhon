import * as colors from "webinate-colors";
import * as proxyServer from "node-http-proxy";
import * as fs from "fs";
import * as http from "http";

colors.log(colors.yellow("Attempting to start up proxy server..."));

// Make sure the config path argument is there
if (process.argv.length < 3)
{
    colors.log(colors.red("No config file specified. Please start noxy with the config path in the argument list. Eg: node Main.js ./config.js"));
    process.exit();
}

// Make sure the file exists
if (!fs.existsSync(process.argv[2]))
{
    colors.log(colors.red(`Could not locate the config file at '${process.argv[2]}'`));
    process.exit();
}

var configPath : string = process.argv[2];
var config: any;

try
{
    // Load config
    config = JSON.parse(fs.readFileSync(configPath, "utf8"));
}
catch (err)
{
    colors.log(colors.red(err));
    process.exit();
}

// Creating the proxy
var proxy = proxyServer.createProxyServer().listen(80);

//
// Create your custom server and just call `proxy.web()` to proxy
// a web request to the target passed in the options
// also you can use `proxy.ws()` to proxy a websockets request
//
var server = http.createServer(function(req, res)
{
    // You can define here your custom logic to handle the request
    // and then proxy the request.
    proxy.web(req, res, { target: config.target });
});
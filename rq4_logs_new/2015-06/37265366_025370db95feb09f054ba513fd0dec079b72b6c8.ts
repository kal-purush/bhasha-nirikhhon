import * as colors from "webinate-colors";
import * as proxyServer from "http-proxy";
import * as fs from "fs";
import * as http from "http";
import {VirtualServer} from "./VirtualServer";
import {IConfigFile} from "./Config";
import * as winston from "winston";

// Create logger
winston.add(winston.transports.File, { filename: 'live-logs.log', maxsize: 50000000, maxFiles: 1, tailable: true });

// Start logging th process
winston.info("Attempting to start up proxy server...", { process: process.pid });

// Make sure the config path argument is there
if (process.argv.length < 3)
{
    winston.error("No config file specified. Please start noxy with the config path in the argument list. Eg: node Main.js ./config.js", { process: process.pid });
    process.exit();
}

// Make sure the file exists
if (!fs.existsSync(process.argv[2]))
{
    winston.error(`Could not locate the config file at '${process.argv[2]}'`, { process: process.pid });
    process.exit();
}

// We have a valid file path, now lets try load it...
var configPath: string = process.argv[2];
var config: IConfigFile;

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
var proxy = proxyServer.createProxyServer();

// Listen for the `error` event on `proxy`.
proxy.on("error", function (err: Error, req: http.ServerRequest, res: http.ServerResponse)
{
    res.writeHead(500, {
        "Content-Type": "text/plain"
    });

    res.end(err.message);
});

try 
{
   

    // Now create each of the virtual servers
    for (var i = 0, l = config.proxies.length; i < l; i++)
        new VirtualServer(proxy, config.proxies[i]);
}
catch (err)
{
    colors.log(colors.red(err));
    process.exit();
}

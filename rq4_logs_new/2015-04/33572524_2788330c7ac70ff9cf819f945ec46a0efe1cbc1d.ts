import express = require("express");
import bodyParser = require("body-parser");
import router = require("./routes/routes");

class TodoListBackendClass {
    server: any;
    expressApp: express.Application;
    mode: String;
    port: Number;

    constructor(mode: String, port: Number){
        if(TodoListBackendClass.portIsValid(port)){
            this.port = port;
        } else {
            throw new Error("TodoListBackend-Constructor: Port must be between 0 and 10000, was: " + port);
        }
        if(TodoListBackendClass.modeIsValid(mode)){
            this.mode = mode;
        } else {
            throw new Error("TodoListBackend-Constructor: Mode must be 'dev', 'test' or 'production'");
        }
    }
    private init():void {
        this.expressApp = express();
        this.expressApp.use(bodyParser.urlencoded({extended: true}));
        this.expressApp.use(bodyParser.json());
        this.expressApp.use(router);
    }
    public launch(callback: ((message: String) => void)):void {
        this.init();
        this.server = this.expressApp.listen(this.port,() => {
            if(typeof(callback) === typeof(Function)) return callback('Backend running on: ' + this.port);
        })
    }
    public quit():void {
        this.server.close();
    }
    private static portIsValid(port: Number):Boolean {
        return (port > 0 && port < 10000)
    }
    private static modeIsValid(mode: String):Boolean {
        return(mode === 'test' || mode === 'dev' || mode === 'production')
    }
}

export = TodoListBackendClass;
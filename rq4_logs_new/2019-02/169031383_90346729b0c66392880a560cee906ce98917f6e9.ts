import * as http from "http";
import * as httpProxy from "http-proxy";

// TODO: Hard code
const httpPort = 8181;

class BlueGreenProxy {
  bgToSymbols = {
    "blue": new Set(),
    "green": new Set()
  };

  bgState: "blue" | "green" = "blue";

  constructor(readonly proxy: httpProxy, readonly blueTarget: string, readonly greenTarget: string){}

  handle(req: http.IncomingMessage, res: http.ServerResponse){
    const currentBgState = this.bgState;
    const symbol = Symbol();
    this.bgToSymbols[currentBgState].add(symbol);
    switch (currentBgState) {
      case "blue":
        proxy.web(req, res, { target: this.blueTarget });
        break;
      case "green":
        proxy.web(req, res, { target: this.greenTarget });
        break;
      default:
        console.error("Unexpected error: not blue or green");
    }

    const deleteSymbol = ()=>{
      this.bgToSymbols[currentBgState].delete(symbol);
      console.log("bgToSymbols: ", this.bgToSymbols);
    };
    req.on("close", ()=>{
      console.log(`req on close`);
      deleteSymbol();
    });
    req.on("end", ()=>{
      console.log(`req on end`);
      deleteSymbol();
    });
    req.on("error", ()=>{
      console.log(`req on error`);
      deleteSymbol();
    });
  
    res.on("close", ()=>{
      console.log(`res on close`);
      deleteSymbol();
    });
    res.on("finish", ()=>{
      console.log(`res on finish`);
      deleteSymbol();
    });
    res.on("error", ()=>{
      console.log(`res on error`);
      deleteSymbol();
    });
  }

  setBgState(bgState: "blue" | "green") {
    this.bgState = bgState;
  }

  toggleBgState() {
    this.bgState = this.bgState === "blue" ? "green" : "blue";
  }
}


const proxy = httpProxy.createProxyServer();

const bgProxy = new BlueGreenProxy(
  proxy,
  // TODO: Hard code
  "http://localhost:8080",
  "http://localhost:9081"
);

const server = http.createServer(function(req, res) {
  bgProxy.handle(req, res);
});


server.listen(httpPort, ()=>{
  console.log(`Listening on ${httpPort}...`);
});

// Not to down whole server
process.on('uncaughtException', function (err) {
  console.error('on uncaughtException: ', err);
});
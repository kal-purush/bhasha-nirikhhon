
// # Peerbelt tests
// 127.0.0.1       ssl-test.peerbelt.com
// 127.0.0.1       pronto.peerbelt.com
// 127.0.0.1       internal.pronto.peerbelt.com
// 127.0.0.1       google.pronto.peerbelt.com
// 127.0.0.1       yarn.pronto.peerbelt.com

var httpProxy = require( "http-proxy" );
var fs = require( "fs" );
//
// Create the HTTPS proxy server in front of a HTTP server
//
var binding = {
    "yarn.pronto.peerbelt.com": {
        target: "https://yarnpkg.com:443",
        "changeOrigin": true
    },
    "google.pronto.peerbelt.com": {
        target: "https://www.google.com:443",
        "changeOrigin": true
    },
    "*.pronto.peerbelt.com": {
        target: "https://www.microsoft.com:443/en-us/",
        "changeOrigin": true
    },
    "pronto.peerbelt.com": {
        target: {
            host: "overlayads.herokuapp.com",
            port: 443,
            protocol: "https"
        },
        changeOrigin: true
    }
};


function socketErrorHandler( err, req ) {
    if ( req && ( req = req.socket ) ) {
        try { 
            req.end('HTTP/1.1 400 Bad Request\r\n\r\n');
        } catch( e ) {
            ;
        } 
    }
};

httpProxy.createServer({
  binding: binding,
  target: {
    host: "localhost",
    port: 3030
  },
  ssl: {
    key: fs.readFileSync("../ssl-certificates/peerbelt.com/STAR_peerbelt_com-2017/peerbelt.com_pk.txt", "utf8"),
    cert: fs.readFileSync("../ssl-certificates/peerbelt.com/STAR_peerbelt_com-2017/peerbelt.com_crt.txt", "utf8")
  }
})
.listen(443)
.on( "error", socketErrorHandler );

httpProxy.createServer({
  binding: binding,
  target: {
    host: "localhost",
    port: 3030
  }
}).listen(80)
.on( "error", socketErrorHandler );
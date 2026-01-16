/*
Program Name == https://youtu.be/Xg4effPSJXk?si=FdnNettapxyc4CHw
Description ===
Author == marzun
Date == 29/08/23
*/
// depecdencies

const http = require('http');
const url = require('url');

// app object -module scaffholding
const app = {};
// configuration
app.config = {
    port: 3000,
};

// create server
app.createServer = () => {
    const server = http.createServer(app.handleReqRes);
    server.listen(app.config.port, () => {
        console.log(`Listening to port Number : ${app.config.port}`);
    });
};
// handle request response
app.handleReqRes = (req, res) => {
    // respose handle
    const parseUrl = url.parse(req.url, true);
    const path = parseUrl.pathname;
    const trimmedPath = path.replace(/^\/+|\/+$/g, '');
    const method = req.method.toLowerCase();
    const queryStream = parseUrl.query;
    const headersObject = req.headers;
    res.end('Hello hii');
};
app.createServer();
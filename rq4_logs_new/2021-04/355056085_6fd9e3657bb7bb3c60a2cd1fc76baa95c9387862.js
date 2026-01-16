var http = require('http');
var fs = require('fs');
var path = require('path');
var url = require("url");

const hostname = '127.0.0.1';
const port = 3000;

var totalPoints = 0;
var questionNumber = 0;
const serveMethods = ['/updatePoints', '/getPoints', '/getQuestionNumber', '/putQuestionNumber'];

const server = http.createServer(function (request, response) {
    console.log('request ', request.url, ' method ', request.method);

    if (!serveMethods.includes(request.url)) {
        var filePath = '.' + request.url;
        if (filePath == './') {
            totalPoints = 0;
            questionNumber = 0;
            filePath = './views/home.html';
        }

        var extname = String(path.extname(filePath)).toLowerCase();
        var mimeTypes = {
            '.html': 'text/html',
            '.js': 'text/javascript',
            '.css': 'text/css',
            '.json': 'application/json',
            '.png': 'image/png',
            '.jpg': 'image/jpg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
            '.wav': 'audio/wav',
            '.mp4': 'video/mp4',
            '.woff': 'application/font-woff',
            '.ttf': 'application/font-ttf',
            '.eot': 'application/vnd.ms-fontobject',
            '.otf': 'application/font-otf',
            '.wasm': 'application/wasm'
        };

        var contentType = mimeTypes[extname] || 'application/octet-stream';

        fs.readFile(filePath, function (error, content) {
            if (error) {
                if (error.code == 'ENOENT') {
                    fs.readFile('./404.html', function (error, content) {
                        response.writeHead(404, { 'Content-Type': 'text/html' });
                        response.end(content, 'utf-8');
                    });
                }
                else {
                    response.writeHead(500);
                    response.end('Sorry, check with the site admin for error: ' + error.code + ' ..\n');
                }
            }
            else {
                response.writeHead(200, { 'Content-Type': contentType });
                response.end(content, 'utf-8');
            }
        });
    } else {
        var resBody;

        const { headers, method, url } = request;
        let body = [];
        request.on('error', (err) => {
            console.error(err);
        }).on('data', (chunk) => {
            body.push(chunk);
        }).on('end', () => {
            body = Buffer.concat(body).toString();
            // BEGINNING OF NEW STUFF

            response.on('error', (err) => {
                console.error(err);
            });

            if (request.method === 'PATCH' && request.url.includes('/updatePoints')) {
                console.log('body: ' + body);
                var jsonObject = JSON.parse(body);
                totalPoints += jsonObject;
                resBody = body;

                console.log('points: ', totalPoints);
            } else if (request.method === 'GET' && request.url.includes('/getPoints')) {
                resBody = totalPoints;

                console.log('totalPoints: ' + totalPoints);
            } else if (request.method === 'GET' && request.url.includes('/getQuestionNumber')) {
                resBody = questionNumber;

                console.log('questionNumber: ' + questionNumber);
            } else if (request.method === 'PUT' && request.url.includes('/putQuestionNumber')) {
                console.log('body: ' + body);
                var jsonObject = JSON.parse(body);
                questionNumber = jsonObject;
                resBody = body;

                console.log('questionNumber: ' + questionNumber);
            }

            response.statusCode = 200;
            response.setHeader('Content-Type', 'application/json');
            // Note: the 2 lines above could be replaced with this next one:
            // response.writeHead(200, {'Content-Type': 'application/json'})

            const responseBody = { headers, method, url, resBody };

            response.write(JSON.stringify(responseBody));
            response.end();
            // Note: the 2 lines above could be replaced with this next one:
            // response.end(JSON.stringify(responseBody))

            // END OF NEW STUFF
        });
    }


}).listen(port, hostname);


// server.on('request', function (request, response) {
//     if (request.url.includes('/updatePoints') || request.url.includes('/getPoints')) {
//         var resBody;

//         const { headers, method, url } = request;
//         let body = [];
//         request.on('error', (err) => {
//             console.error(err);
//         }).on('data', (chunk) => {
//             body.push(chunk);
//         }).on('end', () => {
//             body = Buffer.concat(body).toString();
//             // BEGINNING OF NEW STUFF

//             response.on('error', (err) => {
//                 console.error(err);
//             });

//             if (request.method === 'PATCH' && request.url.includes('/updatePoints')) {
//                 totalPoints += parseInt(JSON.parse(body).points);
//                 resBody = body;
//             } else if (request.method === 'GET' && request.url.includes('/getPoints')) {
//                 resBody = totalPoints;
//             }

//             response.statusCode = 200;
//             response.setHeader('Content-Type', 'application/json');
//             // Note: the 2 lines above could be replaced with this next one:
//             // response.writeHead(200, {'Content-Type': 'application/json'})

//             const responseBody = { headers, method, url, resBody };

//             response.write(JSON.stringify(responseBody));
//             response.end();
//             // Note: the 2 lines above could be replaced with this next one:
//             // response.end(JSON.stringify(responseBody))

//             // END OF NEW STUFF
//         });
//     }
// });

// console.log('Server running at ' + hostname + '/' + port);



// http.createServer((request, response) => {
//     var resBody;

//     const { headers, method, url } = request;
//     let body = [];
//     request.on('error', (err) => {
//         console.error(err);
//     }).on('data', (chunk) => {
//         body.push(chunk);
//     }).on('end', () => {
//         body = Buffer.concat(body).toString();
//         // BEGINNING OF NEW STUFF

//         response.on('error', (err) => {
//             console.error(err);
//         });

//         if (request.method === 'PATCH' && request.url.includes('/updatePoints')) {
//             totalPoints += parseInt(JSON.parse(body).points);
//             resBody = body;
//         } else if (request.method === 'GET' && request.url.includes('/getPoints')) {
//             resBody = totalPoints;
//         }

//         response.statusCode = 200;
//         response.setHeader('Content-Type', 'application/json');
//         // Note: the 2 lines above could be replaced with this next one:
//         // response.writeHead(200, {'Content-Type': 'application/json'})

//         const responseBody = { headers, method, url, resBody };

//         response.write(JSON.stringify(responseBody));
//         response.end();
//         // Note: the 2 lines above could be replaced with this next one:
//         // response.end(JSON.stringify(responseBody))

//         // END OF NEW STUFF
//     });


// }).listen(8080, hostname); // Activates this server, listening on port 8080.
// console.log('Server running at ' + hostname + '/' + 8080);

// function setupResponse(code, body) {

// }
export async function logMiddleware(req: any, res: any, next: Function)
{
    var oldWrite = res.write;
        var oldEnd = res.end;
        
        var log = {
            createdAt: new Date(),
            request: {
                headers: req.headers,
                method: req.method,
                url: req.url,
                params: req.params,
                query: req.query,
                body: req.body
            },
            response: {
                headers: null,
                statusCode: null,
                body: null
            },
        };

        var chunks = [];

        res.write = function (chunk) {
            chunks.push(chunk);

            oldWrite.apply(res, arguments);
        };

        res.end = function (chunk) {
            if (chunk)
                chunks.push(chunk);

            log.response.body = Buffer.concat(chunks).toString('utf8');
            log.response.headers = res._headers;
            log.response.statusCode = res.statusCode;
            console.log(log);
            oldEnd.apply(res, arguments);
        };

        next();
}
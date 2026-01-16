import http, { IncomingMessage, ServerResponse } from 'http';
import listen from 'test-listen';
import { apiResolver } from 'next/dist/next-server/server/api-utils';
import { NextApiHandler } from 'next';

export default async (handler: NextApiHandler) => {
    const requestHandler = (req: IncomingMessage, res: ServerResponse) =>
        //@ts-ignore
        apiResolver(req, res, undefined, handler);

    const server = http.createServer(requestHandler);
    const url = await listen(server);

    return { server, url };
};
import Http from "http";
import Express from "express";

type Handler = (req: Http.IncomingMessage, res: Http.ServerResponse) => Promise<void>;

export function setupExpressServer(pageHandler: Handler, port = 3030): [Express.Express, Http.Server] {

  const appExpress = Express();
  const httpServer = Http.createServer(appExpress);

  // express binding
  appExpress.all("*", (req: Express.Request, res: Express.Response) => {
    if (req.path.includes('sushi')) {
      return res.json({ name: 'maguro' });
    }
    return pageHandler(req, res);
  });

  httpServer.listen(port, (err?: any) => {
    if (err) throw err;
    console.log(`> Ready on localhost:${port} - env ${process.env.NODE_ENV}`);
  });

  return [appExpress, httpServer];
}
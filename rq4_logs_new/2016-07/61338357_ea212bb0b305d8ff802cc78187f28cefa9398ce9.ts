import {Request, Response} from 'express';
import {routes} from './routes';
import renderIndex from './index';
import {match} from 'react-router';

export function get (req: Request, res: Response) {
    match({routes, location: req.url}, (error, redirectLocation, renderProps) => {
        if (error) {
            res.status(500).send(error.message);
        } else if (redirectLocation) {
            res.redirect(302, redirectLocation.pathname + redirectLocation.search);
        } else if (renderProps) {
            res.status(200).send(renderIndex(renderProps))
        } else {
            res.status(404).send('Not found');
        }
    });
}
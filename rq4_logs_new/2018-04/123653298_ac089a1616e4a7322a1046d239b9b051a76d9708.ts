import {Router, Request, Response, NextFunction} from 'express';
import * as fs from 'fs';
import * as path from 'path';
import * as mime from 'mime-types';

let router = Router();

router.route('/:filename')
    .get((req: Request, res: Response, next: NextFunction) => {
        try {
            let filepath = path.resolve('./src/uploads/' + req.params.filename);
            let image = fs.readFileSync(filepath);
            let extension: string = path.extname(req.params.filename);
            let mimeType: any = mime.lookup(extension);
            if (mimeType != null) {
                res.writeHead(200, {'Content-Type': mimeType});
                res.end(image);
            } else {
                res.sendStatus(404);
            }
        } catch (err) {
            res.sendStatus(404);
        }
    });

export let imageRouter = router;
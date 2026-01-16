import * as path from 'path';
import * as fs from 'fs';
import {isUndefined} from "util";

enum HttpStatus {
    Success = 200,
    Created = 201,
    BadRequest = 400,
    NotFound = 404,
    ServerError = 500
}

type HttpReturn = {status: HttpStatus, detail?: any, content?: any};

export class DBDriver {
    private dbPath: string;

    constructor(documentPath) {
        this.dbPath = path.resolve(documentPath);
    }

    public readDoc(documentPath): HttpReturn {
        let fullPath = this.pathCleaner(documentPath);
        if (typeof fullPath !== 'string') {
            return fullPath;
        }
        if (!fs.existsSync(fullPath)) {
            return {status: HttpStatus.NotFound, detail: `file not found at '${documentPath}'`};
        }
        let content = fs.readFileSync(fullPath,"utf-8");
        return {status: HttpStatus.Success, content: content};
    }

    public updateDoc(documentPath, content): HttpReturn {
        let fullPath = this.pathCleaner(documentPath);
        if (typeof fullPath !== 'string') {
            return fullPath;
        }
        buildPath(fullPath);
        fs.writeFileSync(fullPath, JSON.stringify(content));
        return {status: HttpStatus.Success, detail: `document '${documentPath}' written.`}
    }

    public deleteDoc(documentPath): HttpReturn {
        let fullPath = this.pathCleaner(documentPath);
        if (typeof fullPath !== 'string') {
            return fullPath;
        }
        if (!fs.existsSync(fullPath)) {
            return {status: HttpStatus.NotFound, detail: `file not found at '${documentPath}'`};
        }
        fs.unlinkSync(fullPath);
        return {status: HttpStatus.Success, detail: `document '${documentPath}' deleted`};


    }

    private pathCleaner(documentPath: string): HttpReturn|string {
        try {
            return this.cleanPath(documentPath)
        } catch (e) {
            return {status: HttpStatus.BadRequest, detail: `Bad path entered: '${documentPath}'`}
        }
    }

    private cleanPath(documentPath: string): string {
        if (path.isAbsolute(documentPath)) {
            documentPath = path.relative(this.dbPath, documentPath);
        }
        documentPath = path.normalize(documentPath);
        if (documentPath.slice(0, 3) === '../') {
            throw new Error("Cannot access paths higher than db");
        }
        return path.resolve(this.dbPath, documentPath);

    }
}

export function DBDriverMiddlewareFactory(dbPath) {
    let db = new DBDriver(dbPath);
    return (req, res, next) => {
        if (req.method === 'GET' && !req.params[0]) {
            let data = db.readDoc(req.params[1]);
            res.status(data.status).send(data);
        } else if (req.method === 'POST' && req.params[0]) {
            let data;
            if (req.params[0] === 'update') {
                data = db.updateDoc(req.params[1], req.body);
            } else {
                data = db.deleteDoc(req.params[1]);
            }
            res.status(data.status).send(data);
        } else {
            res.status(HttpStatus.BadRequest).send({
                status: HttpStatus.BadRequest,
                detail: "bad request parameters",
            });
        }
        next()
    }
}

//builds parent directories, assumes normalized absolute path
function buildPath(filePath) {
    let parsedFilePath = path.parse(filePath);
    if (parsedFilePath.dir === parsedFilePath.root)
        return;
    buildPath(parsedFilePath.dir);
    if (!fs.existsSync(parsedFilePath.dir)) {
        fs.mkdirSync(parsedFilePath.dir);
    }
};
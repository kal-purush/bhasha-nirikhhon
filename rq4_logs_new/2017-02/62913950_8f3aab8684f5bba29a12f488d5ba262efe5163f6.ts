import * as eta from "eta-lib";

import * as express from "express";

export class Model implements eta.Model {
    public render(req: express.Request, res: express.Response, callback: (env: { [key: string]: any }) => void): void {
        let Jimp = require("jimp");
        let firstName: string = req.body.name.split(" ")[0]
        Jimp.read(eta.server.modules["office"].baseDir + "lib/nametag/horizontal.jpg", function (err: any, image: any) {
            if (err) {
                eta.logger.error(err);
                return callback({errcode: eta.http.InternalError});
            }
            Jimp.loadFont(Jimp.FONT_SANS_32_WHITE).then(function (font: any) {
                image.print(font, 11, 9, firstName);
                image.rotate(270, true);
                image.resize(144, 253);
                image.write(eta.server.modules["office"].baseDir + "lib/nametag/horizontal-" + req.body.name + ".jpg");
            });
        });

        let canvas: any = <HTMLScriptElement>document.getElementById('canvas');
        let ctx = canvas.getContext('2d');
        ctx.font = 16 + "pt Open Sans";
        let textWidth = ctx.measureText(name).width;

        let startPos = (144 - textWidth)/2;

        Jimp.read(eta.server.modules["office"].baseDir + "lib/nametag/vertical.jpg", function (err: any, image: any) {
            if (err) {
                eta.logger.error(err);
                return callback({errcode: eta.http.InternalError});
            }
            Jimp.loadFont(Jimp.FONT_SANS_16_WHITE).then(function (font: any) {
                image.print(font, startPos, 150, firstName);
                image.write(eta.server.modules["office"].baseDir + "lib/nametag/verticalwithname.jpg");
                callback({});
            });
        });
    }
}
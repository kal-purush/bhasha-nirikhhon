import { Secure } from './_secure.helper';
import { QuoteService } from '../services/quote.serv'
import { Router } from 'express';
import * as pdf from 'html-pdf';
import fs = require("fs");
import url = require('url');
import { IQuote } from '../models/document/quote';
import { ApplicationSetting } from '../config';

const router: Router = Router();

router.get('/pdf/quote', async (req, res) => {
    const params: { id: string, typedocument: string } = <any>url.parse(req.url, true).query;

    if (params.id != null && params.id != undefined && params.id != "") {
        let servQuote: QuoteService = new QuoteService();
        let quotes: IQuote[] = await servQuote.get(<IQuote>{ _id: params.id });

        let html: string = "<html><head><link rel='stylesheet' href='" + ApplicationSetting.CssDocument + "'></head><body>";
        html += quotes[0].html;
        html += "</body></html>"
        pdf.create(html, { format: 'Letter' }).toStream(function (err, stream) {
            if (err) {
                res.json({
                    message: 'Sorry, we were unable to generate pdf',
                });
            }

            stream.pipe(res); // your response
        });
    }
    else {
        let html: string = "<html><head><link rel='stylesheet' href='" + ApplicationSetting.CssDocument + "'></head><body>";
        html += "Apercu disponible après la création."
        html += "</body></html>"
        pdf.create(html, { format: 'Letter' }).toStream(function (err, stream) {
            if (err) {
                res.json({
                    message: 'Sorry, we were unable to generate pdf',
                });
            }

            stream.pipe(res); // your response
        });
    }
});

router.get('/pdf/html/template/quote', Secure.authenticate, async (req, res) => {
    res.sendFile(ApplicationSetting.HtmlDocumentTemplateDirectory + "/quote.html");
});

router.get('/pdf/document-pdf', async (req, res) => {
    fs.readFile('src/styles/document-pdf.css', function (err, data) {
        if (err) {
            throw err;
        }
        res.writeHead(200, { 'Content-Type': 'text/css' });
        res.end(data);
        return;
    });
});

router.get('/pdf/document-html', async (req, res) => {
    fs.readFile('src/styles/document-html.css', function (err, data) {
        if (err) {
            throw err;
        }
        res.writeHead(200, { 'Content-Type': 'text/css' });
        res.end(data);
        return;
    });
});

router.post('/v1/image', async (req, res) => {
    console.log("okokokok");
    res.send(200);
});

export default router;
import { NextApiRequest, NextApiResponse } from 'next';
import puppeteer from 'puppeteer';



async function printPDF() {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto("https://www.google.com/", { waitUntil: "networkidle0" });
    const pdf = await page.pdf({ format: 'A4' });
    await browser.close();
    return pdf;
}

type Data = {

}

export default function handler(
    req: NextApiRequest,
    res: NextApiResponse<any>
) {
    //#testPdf
    /* return pdf */
    printPDF().then((pdf) => {
        res.status(200).send(pdf);
    });
}
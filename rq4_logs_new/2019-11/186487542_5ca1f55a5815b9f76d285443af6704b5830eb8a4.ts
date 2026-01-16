/*!
 * Source https://github.com/donmahallem/TrapezeApiTypes
 */

import { createTrapezeApiRoute } from "@donmahallem/trapeze-api-express-route";
import * as express from "express";
import { Server } from "http";
import { difference, uniq } from "lodash";
import { exists, mkdir, readFile, writeFile } from "mz/fs";
import { dirname, join } from "path";
import * as puppeteer from "puppeteer";

// defining some configuration
const PORT = 4000;
const HOST = `http://localhost:${PORT}`;

let PAGES: string[] = ["TrapezeClientNgDemo/stops"];
let RENDERED_PAGES: string[] = [];
const timeout: (ms: number) => Promise<any> = (ms: number): Promise<any> =>
    new Promise((resolve) => setTimeout(resolve, ms));
const main = async () => {

    // starting an Express.js server to serve the static files while puppeter prerender the pages
    const app = express();

    // setting the html content from the index.html file
    const index = (await readFile(join(process.cwd(),
        "TrapezeClientNg",
        "dist",
        "trapeze-client-ng",
        "index.html"))).toString();
    const subRoute = express.Router();
    subRoute.use("/api", createTrapezeApiRoute("https://kvg-kiel.de"));
    subRoute.get("*.*", express.static(join(__dirname, "..",
        "TrapezeClientNg",
        "dist",
        "trapeze-client-ng"), {

        }));
    subRoute.get("*", (req, res) => res.send(index));
    app.use("/TrapezeClientNgDemo", subRoute);

    // starting the express server
    const server: Server = await (new Promise((resolve, reject) => {
        const s = app.listen(PORT, (e) => e ? reject(e) : resolve(s));
    }));

    // tslint:disable-next-line:no-console
    console.log(`Started server ${HOST}!`);

    // launching Puppeteer
    const browser = await puppeteer.launch();
    await timeout(1000);
    // tslint:disable-next-line:no-console
    console.log(`Started browser!`);

    // creating a new Tap/Page
    const page = await browser.newPage();

    do {
        const p = PAGES[0];

        // requesting the first page in PAGES array
        await page.goto(`${HOST}/${p}`, { waitUntil: "networkidle0" });

        // getting the html content after the Chromium finish rendering.
        let result = await page.evaluate(() => document.documentElement.outerHTML);
        result = await page.content();
        // defining the html file name that will be created
        let file = "";
        if (p) {
            file = join(process.cwd(), "prerender", p, "index.html");
        } else {
            file = join(process.cwd(), "prerender", "index.html");
        }
        const dir = dirname(file);

        // test if the directory exist, if not create the directory
        if (!(await exists(dir))) {
            await mkdir(dir, { recursive: true } as any);
        }

        // write the rendered html file
        await writeFile(file, result);

        // tslint:disable-next-line:no-console
        console.log(`Wrote ${file}`);

        // add this page to the RENDERED PAGES array
        RENDERED_PAGES = [...RENDERED_PAGES, p];

        // set PAGES with the pages that still need to be rendered

        /// uniq(PAGES.concat(result.match(/href="\/[\/\w\d\-]*"/g).map(s => s.match(/\/([\/\w\d\-]*)/)[1]))),
        const matchedUrls: RegExpMatchArray | null = result.match(/href="\/[\/\w\d\-]*"/g);
        if (matchedUrls) {
            const matchedPath = matchedUrls.map((s: string) => {
                const match = s.match(/\/([\/\w\d\-]*)/);
                if (match) {
                    return match[1];
                }
                return "";
            });
            PAGES = difference(
                uniq(PAGES.concat(matchedPath)),
                RENDERED_PAGES,
            );
        }

    } while (PAGES.length > 0);

    // closes Chromium and finishes the express server.
    browser.close();
    server.close();
};

// run the main asyn function
main()
    // tslint:disable-next-line:no-console
    .then(() => console.log("All right!"))
    .catch((err) => {
        // tslint:disable-next-line:no-console
        console.error("Err", err);
        process.exit(1);
    });
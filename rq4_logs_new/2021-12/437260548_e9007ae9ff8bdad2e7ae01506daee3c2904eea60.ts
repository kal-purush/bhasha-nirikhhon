import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  rmSync,
  copyFileSync,
} from "fs";
import { join } from "path";
import { genThreadHTML, genThread, genTweetHTML } from "twitter-threads";


const tweets = [
  "1467308938282500096", "1461617488848834563",
  "1459624305860378624", "1457409282698190851", "1451743926465884160",
  "1449855648334708742", "1444441812840505349", "1441841488464257028",
  "1439043099473235968", "1410524280362717188", "1405745451500900361",
];

const outDir = "dist"
const threadsCSS = "threads.css"

let tweetHTMLs = "";

if (existsSync(outDir)) {
  rmSync(outDir, {
    recursive: true,
    force: true
  });
}

mkdirSync(outDir);
copyFileSync(threadsCSS, join(outDir, threadsCSS));

genRun();

async function genRun() {
  for (const id of tweets) {
    await genProcessTweet(id);
  }
  parseToTemplate("index", tweetHTMLs);
}

async function genProcessTweet(id: string) {
  const html = await genThreadHTML(id);
  const firstID = (await genThread(id))[0].id_str;
  tweetHTMLs = tweetHTMLs + (await genTweetHTML(firstID))
    .replace("https://twitter.com/binhonglee/status/" + firstID, "./" + id + ".html")
    .replace("Read more...", "Read this thread...");
  parseToTemplate(id, html);
}

function parseToTemplate(id: string, html: string) {
  writeFileSync(
    join(outDir, id + ".html"),
    readFileSync("template.html")
      .toString()
      .replace(
        "{{ $CONTENT }}",
        html.split("\n").join("\n          ")
      )
    );
}
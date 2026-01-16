import puppeteer from "puppeteer";
import * as cheerio from "cheerio";

const browser = await puppeteer.launch();
const page = await browser.newPage();

const baseUrl = "https://www.youtube.com/@KARAOKEKY/search";

async function scrapeSongNumber(query: string) {
  const searchUrl = `${baseUrl}?query=${encodeURIComponent(query)}`;
  await page.goto(searchUrl);

  const html = await page.content();
  const $ = cheerio.load(html);
  // id contents 의 첫번째  ytd-item-section-renderer 찾기
  const firstItem = $("#contents ytd-item-section-renderer").first();
  // yt-formatted-string 찾기
  const title = firstItem.find("yt-formatted-string").first().text().trim();

  const karaokeNumber = extractKaraokeNumber(title);

  // await browser.close();
  return karaokeNumber;
}

function extractKaraokeNumber(title: string) {
  // KY. 찾고 ) 가 올때까지 찾기
  const matchResult = title.match(/KY\.\s*(\d{3,5})\)/);
  const karaokeNumber = matchResult ? matchResult[1] : null;
  return karaokeNumber;
}

// 사용
const result = await scrapeSongNumber("눈물이 되어줄게 - 허각");
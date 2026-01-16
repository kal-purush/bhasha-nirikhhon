import { htmlToImage, htmlToPdf, RESOLUTION } from "./puppeteer.ts";

// Generate screenshots and PDFs of `https://cv.nunorodrigues.tech/` and `generated/cv.html` to be used for visual comparison.
// Note this script only generates screenshots and PDFs from HTML. The visual comparison is done elsewhere.
async function main() {
  const basePath = new URL(`../generated`, import.meta.url).pathname;

  const currentUrl = `https://cv.nunorodrigues.tech/`;
  const newUrl = `file://${basePath}/cv.html`;

  await Promise.all([
    htmlToImage(currentUrl, `${basePath}/current.hd.dark.png`, RESOLUTION.HD, `dark`),
    htmlToImage(currentUrl, `${basePath}/current.hd.light.png`, RESOLUTION.HD, `light`),
    htmlToImage(currentUrl, `${basePath}/current.mobile.dark.png`, RESOLUTION.MOBILE, `dark`),
    htmlToImage(currentUrl, `${basePath}/current.mobile.light.png`, RESOLUTION.MOBILE, `light`),
    htmlToPdf(currentUrl, `${basePath}/current.pdf`),
    htmlToImage(newUrl, `${basePath}/new.hd.dark.png`, RESOLUTION.HD, `dark`),
    htmlToImage(newUrl, `${basePath}/new.hd.light.png`, RESOLUTION.HD, `light`),
    htmlToImage(newUrl, `${basePath}/new.mobile.dark.png`, RESOLUTION.MOBILE, `dark`),
    htmlToImage(newUrl, `${basePath}/new.mobile.light.png`, RESOLUTION.MOBILE, `light`),
    htmlToPdf(newUrl, `${basePath}/new.pdf`),
  ]);
}

await main();
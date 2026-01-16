import puppeteer from 'puppeteer';

export async function fetchHtml(url: string) {
  const browser = await puppeteer.launch({
    // executablePath: '/usr/bin/google-chrome-stable',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();
  await page.goto(url);

  const html = await page.content();
  
  await browser.close();

  return html;
}
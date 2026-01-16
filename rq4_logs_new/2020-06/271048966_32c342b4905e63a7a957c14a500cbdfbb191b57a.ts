const request = require("request");
const cheerioModule = require("cheerio");

export default function getInawera(maxPages: number) {
  let currentPage = 1;
  const flavors = [{}];

  while (currentPage <= maxPages) {
    const targetPage = currentPage === 1 ? currentPage : currentPage + 1;

    console.log(`Scraping page ${targetPage}`);
    let url = `http://www.inaweraflavours.com/en/7-e-flavours?n=50&id_category=7&p=${targetPage}`;

    currentPage += 1;
    request(
      {
        method: "GET",
        url: url,
      },
      (err, res, body) => {
        const $ = cheerioModule.load(body);
        const flavorsOnPage = [{}];

        $("h3").each(function () {
          const flavor = $(this);
          flavorsOnPage.push({
            name: flavor
              .text()
              .trim()
              .replace(/(\r\n|\n|\r)/gm, ""),
          });
        });
        flavorsOnPage.shift(); // removed empty arr
        flavorsOnPage.shift(); // removes "subcategories " element
        console.log(flavorsOnPage);
        flavors.push(flavorsOnPage);
      }
    );
  }
}
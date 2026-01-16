const { createWriteStream } = require("fs");
const { SitemapStream, streamToPromise } = require("sitemap");

// Define the URLs to include in the sitemap
const urls = [
  { url: "/", changefreq: "daily", priority: 1.0 },
  { url: "/prestations", changefreq: "daily", priority: 0.9 },
  { url: "/contact", changefreq: "daily", priority: 0.8 },
  { url: " /prestations#prestation-1", changefreq: "daily", priority: 0.7 },
  { url: " /prestations#prestation-2", changefreq: "daily", priority: 0.7 },
  { url: " /prestations#prestation-3", changefreq: "daily", priority: 0.7 },
  { url: " /prestations#qvt", changefreq: "daily", priority: 0.7 },
];

// Create a sitemap stream with the specified hostname
const sitemap = new SitemapStream({
  hostname: "https://www.massage-stephanie-heudre.fr/",
});

// Create a write stream for the sitemap.xml file
const writeStream = createWriteStream("./public/sitemap.xml");
sitemap.pipe(writeStream);

// Write the URLs to the sitemap stream
urls.forEach((url) => sitemap.write(url));

// End the sitemap stream
sitemap.end();

// Convert the sitemap stream to a promise to handle success and errors
streamToPromise(sitemap)
  .then(() => {
    console.log("Sitemap generated successfully!");
  })
  .catch((err: unknown) => {
    console.error("Error generating sitemap:", err);
  });
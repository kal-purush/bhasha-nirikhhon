const request = require("request");
const cheerio = require("cheerio");
const fs = require("fs");
const Promise = require("bluebird");

const url = "https://www.cermati.com";
const jsonValue = {};

getAllArticles();

async function getAllArticles() {
	console.log("Loading...");

	const data = await getHTMLFromLink(url + "/artikel");
	const articleDetails = await getArticleDetails(data);
	jsonValue["articles"] = articleDetails;

	console.log("Extracting data to file solution.json ...");
	fs.writeFileSync("solution.json", JSON.stringify(jsonValue));
	console.log("Completed...");
}

function getHTMLFromLink(linkUrl) {
	return new Promise(resolve => {
		request(linkUrl, (error, response, body) => {
			resolve(body);
		});
	});
}

function getArticleDetails(data) {
	return new Promise(resolve => {
		const htmlData = cheerio.load(data);
		const arrayValue = [];

		htmlData(".article-list-item", ".list-of-articles").each((i, el) => {
			const href = htmlData(el).children("a").attr("href");
			// console.log(i, href);
			arrayValue.push(getEach(url + href));
		});

		Promise.all(arrayValue)
			.then((results) => {
				resolve(results);
			});
	});
}

function getEach(linkUrl) {
	return new Promise(resolve => {
		request(linkUrl, (error, response, body) => {
			const htmlData = cheerio.load(body);

			const title = htmlData(".post-title").text().replace(/\s\s+/g, "");
			const author = htmlData(".author-name").text().replace(/\s\s+/g, "");
			const postingDate = htmlData(".post-date").children("span").text().replace(/\s\s+/g, "");

			const arrayRelated = [];
			const data = cheerio.load(htmlData(".panel-items-list", ".side-list-panel").html());
			data("li").each((i, el) => {
				const urlRelated = data(el).children("a").attr("href");
				const titleRelated = data(el).children("a").children("h5").text().replace(/\s\s+/g, "");

				const jsonRelated = {
					"url": url + urlRelated,
					"title": titleRelated
				};

				arrayRelated.push(jsonRelated);
			});

			const jsonValue = {
				"url": linkUrl,
				"title": title,
				"author": author,
				"postingDate": postingDate,
				"relatedArticles": arrayRelated
			};

			resolve(jsonValue);
		});
	});
}
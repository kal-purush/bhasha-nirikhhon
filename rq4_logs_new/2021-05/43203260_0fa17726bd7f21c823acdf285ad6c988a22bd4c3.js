import fs from "fs";

const text = fs.readFileSync(0, "utf8");
const mapping = JSON.parse(text);

const base = "https://ubistatic-a.akamaihd.net/0018/base/GFX_HASHED";
const pattern = /^building_lib\/(buffad_)?c(ollectible)?_[a-z0-9_]*\.png$/;
const replacement = "f9f7e2bacd84c76001820a3621bda5c6959d609d.png";

const rules = [];
for (const [file, hash] of mapping) {
	if (file.match(pattern)) {
		rules.push({
			"id": rules.length + 1,
			"condition": { "urlFilter": `${base}/building_lib/${hash}`, "resourceTypes": ["xmlhttprequest"] },
			"action": { "type": "redirect", "redirect": { "url": `${base}/building_lib/${replacement}` } }
		});
	}
}

console.log(JSON.stringify(rules, null, 2));
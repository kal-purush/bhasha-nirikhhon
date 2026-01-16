let regions = require('./regions.json');
let rp = require('request-promise');
let fs = require('fs');

let goodWiki = [];
let badWiki = [];

String.prototype.toProperCase = function () {
  return this.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
};

for (let region of regions.features) {
  let name = region.properties.name.toProperCase();
  checkWiki(name, region);
}

async function checkWiki(name, region) {
  try {
    let response = await requestWiki(name);
    addNames(response, name, region);
  } catch (error){
    console.error(error);
  }
}

process.on('exit', finish.bind());

function addNames(resp, name, region) {
  if (resp.query.pages['-1']) {
    badWiki.push(region);
  } else {
    for (let page in resp.query.pages) {
      region["wikiName"] = resp.query.pages[page].title;
      goodWiki.push(region);
      console.log()
    }
  }
}

function requestWiki(name) {
  let url = ['https://en.wikipedia.org/w/api.php?action=query&prop=extracts&redirects=1&format=json&titles=', name].join('');
  return rp({
    uri: url,
    json: true
  });
}

function writeFiles (name, data) {
  fs.writeFileSync(name, data, function (err) {
    if (err) {
      return console.log(err);
    }
    console.log("The file was saved!");
  }); 
}

function finish() {
  writeFiles('badWiki.json', JSON.stringify(badWiki));
  writeFiles('goodWiki.json', JSON.stringify(goodWiki));
}
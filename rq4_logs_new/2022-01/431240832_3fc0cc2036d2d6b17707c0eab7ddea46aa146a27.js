const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

// https://codeburst.io/javascript-async-await-with-foreach-b6ba62bbf404
async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

module.exports = async () => {
  const pageDirectory = path.join(process.cwd(), "pages");
  const entries = await fsp.readdir(pageDirectory, { withFileTypes: true });
  //console.log("pageDirectory", pageDirectory);
  //console.log("entries", entries);

  const onlyDirs = entries.filter((de) => de.isDirectory()).map((de) => de.name);

  //console.log(onlyDirs);


  const paths = new Map();
  paths.set(
    "documentation",
    entries
      .filter((de) => de.isFile())
      .filter((de) => de.name.endsWith(".mdx"))
      .map((de) => de.name)
  );
  await Promise.all(
    onlyDirs
      //.map((de) => de.name)
      .map(async (dirname) => {
        paths.set(dirname, await fsp.readdir(path.join(pageDirectory, dirname)));
      })
  );
  //console.log(paths)

  const names = {
    "documentation": "Documentation",
    "interactions": "Interactions",
    "resources": "Resources",
    "topics": "Topics",
    "game-and-server-management": "Game & Server Management",
    "rich-presence": "Rich Presence",
    "game-sdk": "Game SDK",
    "dispatch": "Dispatch"
  };

  let info = {};

  await asyncForEach(Array.from(paths.keys()), async (category) => {
    info[category] = {
      name: names[category],
      items: await paths.get(category).reduce(async (obj, page) => {
        const filePath = path.join(pageDirectory, (category === "documentation" ? "" : category), page);
        let first = true;
        const content = (await fsp.readFile(filePath, "utf8")).split(/\r?\n/).filter((line) => {
          if (line.startsWith("## ") || (line.startsWith("# ") && first)) {
            first = false;
            return true;
          } else {
            return false;
          }
        });
        //console.log(category, page, content);
        return {
          ...await obj,
          [page]: {
            title: content[0].substr(2),
            sublinks: content.slice(1).map(line => line.substr(3))
          }
        };
      }, {})
    };
  });

  //console.log("ZZZ", info);
  //console.log("ZZZ", JSON.stringify(info));

  return {
    // cacheable: true,
    code: `module.exports = ${JSON.stringify(info)}`
  };
};
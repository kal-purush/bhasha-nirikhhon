declare var require, process, exports;

let ajax = require("najax");
let jsyaml = require("./js-yaml.min.js");

let fs =require("fs")

exports.loadCardYamlStats = (post) => {
    function rawPost(content) {
        post(jsyaml.safeLoad(content));
    }
    return rawPost(fs.readFileSync("./cards.yaml"));
    ajax({
        url: "https://raw.githubusercontent.com/ludamad/card-rules/master/cards.yaml",
        // Set to a default on error:
        error: (err) => rawPost(`
    Goblin:\n          Gold: 2\n          Health: 2\n          Attack: 1\n          Movement: 2\n          Range: 1\n          Description: A short and fast goblin.\n          Traits: [Humanoid]
    `)
    }).done(rawPost);
}
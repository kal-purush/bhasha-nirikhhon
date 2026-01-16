// import gccx from "gccx";
const gccx = require("gccx").default;
const fs = require("fs");
const path = require("path");

// console.log('gccx', gccx);

const readData = (filename) => {
    return new Promise((resolve, reject) => {
        fs.readFile(filename, 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                reject(err);
                return;
            }
            resolve(data);
        });
    })
};

const files = process.argv.slice(2);
if (files.length === 0) {
    console.error('empty file list');
    process.exit(1);
}
// const patten = /\/\*[^*]*\*\//gims;
// const patten = /[a-z]\s/gims;
// const patten = /GCCX\s/gims;
const patten = new RegExp("GCCX", "g");
(async () => {
    for (const file of files) {
        const name = path.basename(file), ext = path.extname(file);
        const new_name = `${name.slice(0, name.lastIndexOf('.'))}_dist${ext}`;
        console.log('file', file, '==>', new_name);
        const fileData = await readData(file);
        let complied = '';
        try {
            complied = gccx.parse(fileData);
        } catch (e) {
            console.error(e);
            continue;
        }
        // console.log(complied);
        const matches = complied.match(new RegExp("GCCX[ \n\t]*/\\*[^*]*\\*/", "gims"));
        for (const match of matches) {
            const replaced = match.replace(new RegExp("GCCX[ \n\t]*/\\*", "gims"), "")
                .replace("*/", "");
            // console.log(replaced);
            complied = complied.replace(match, replaced);
        }
        // console.log(complied);
        fs.writeFile(new_name, complied, err => {
            err && console.error(err);
        })
    }
})();

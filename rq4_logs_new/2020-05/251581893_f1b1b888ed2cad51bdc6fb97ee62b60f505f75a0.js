const { tree, } = require('./tree.js');

let path = process.argv[2].trim();
if (path.startsWith('\'') || path.startsWith('\"'))
    path = path.substr(1);
if (path.endsWith('\'') || path.endsWith('\"'))
    path = path.substr(0, path.length - 1);

tree(path).then(({ files, folders, }) => {
    files.forEach(f => {
        console.log(f);
    });
    console.log('---');
    folders.forEach(f => {
        console.log(f);
    });
});
const franc = require('franc');
const langs = require('langs');
const colors = require('colors');

const input = process.argv[2];
const langCode = franc(input);
// console.log(langCode);

if (langCode === 'und') {
    console.log("Sorry, could not figure out the language! Try with more text!".red)
}
else {
    const language = langs.where('3', langCode);
    if (language) {
        console.log(`My best guess is: ${language.name}`.green)
    }
    else {
        console.log(`Sorry, could not find a language for the code: ${langCode}`.red)
    }
}
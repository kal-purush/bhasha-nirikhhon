const franc = require('franc');
const langs = require('langs');
const colors = require('colors');

const input = process.argv[2];

const langCode = franc(input);
// const langCode = franc('Alle menslike wesens word vry');
if (langCode === 'und'){
    console.log('解析できませんでした。違う文章でもう一度試してください。'.red);
} else {
    const language = langs.where('3', langCode);
    console.log(`${language.name}でしょうか？`.green);
}

// const language = langs.where('3', 'langCode');
// console.log(language);
// console.log(language.name);


//node index.js '日本語の文章を渡してみる'
//Japanese
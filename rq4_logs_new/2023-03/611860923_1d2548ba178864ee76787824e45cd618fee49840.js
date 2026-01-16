let Parser = require('rss-parser');
let parser = new Parser();
let fs = require('fs')
let url = 'https://www.ledevoir.com/rss/section/environnement.xml'


const keywords = []
fs.readFileSync('keywords.txt', 'utf8').split('\n').forEach(word=>{
    keywords.push(word.replace(/(\r\n|\n|\r)/gm, "").toLowerCase())
});
console.log(keywords)

let grabRss = function(url){
    (async () => {
        let feed = await parser.parseURL(url);
                
        feed.items.forEach(item => {
            let title = item['title'].toLocaleLowerCase()
            let content = item['content'].toLocaleLowerCase()
            
            keywords.forEach(keyword=>{
                if (title.includes(keyword) || title.includes(keyword) ){
                    console.log(item)
                }

                
            });
            
        });
      })();
};
grabRss(url       )
const fs    = require('fs');
const mode  = process.argv[2];
const dir   = process.argv[3];

// create html for Sophie's web galleries
// how to use:
// $ node index.js work ~/Pictures/etc
// $ node index.js news ~/Pictures/etc
fs.readdir(dir, function(err, data) {
  if (err) {
    return err;
  }

  let dirString = (mode === 'work') ? 'work' : 'news';
  
  data.forEach((file) => {
    console.log(`<div class="image-wrapper"><img src="/images/news/${dirString}${file}" alt="${file}" /></div>`);
  });
});
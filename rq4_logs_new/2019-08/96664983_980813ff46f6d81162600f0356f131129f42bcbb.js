const fs = require("fs");
const glob = require("glob");
const path = require("path");
const sharp = require("sharp");
const reduceImg = require('gulp-imagemin');
const renameImg = require('gulp-rename');


const transforms = [
    {
        src: './site/assets/images/**/*.+(jpg|png)',
        dist: './docs/assets/images/',

        options: {
            width: 800
        },
        quality: 80,
        size: 'med'
    },
    {
        src: './site/assets/images/**/*.+(jpg|png)',
        dist: './docs/assets/images/',
        options: {
            width: 400
        },
        quality: 60,
        size: 'sm'
    }
]

function resizeImages(done) {
    // loop through configuration array of objects
    transforms.forEach(function(transform) {
      // if dist folder does not exist, create it with all parent folders
      if (!fs.existsSync(transform.dist)) {
        fs.mkdirSync(transform.dist, { recursive: true }, (err) => {
          if (err) throw err;
        });
      }
  
      // glob all files
      let files = glob.sync(transform.src);
  
      // for each file, apply transforms and save to file
      files.forEach(function(file) {
        let filename = path.basename(file);
        sharp(file)
          .resize(transform.options)
          .jpeg({ quality: transform.quality })
          .toFile(`${transform.dist}/${filename}-${transform.size}`)
          .catch(err => {
            console.log(err);
          });
      });
    });
    done();
  }
  
  // exports (Common JS)
  module.exports = {
    resize: resizeImages
  };
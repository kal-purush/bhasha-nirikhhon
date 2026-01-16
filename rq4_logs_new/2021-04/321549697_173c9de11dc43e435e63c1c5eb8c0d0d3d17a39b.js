const fs = require("fs");
getAllDirFiles = (dirPath, oldestFile) => {
  files = fs.readdirSync(dirPath);
  oldestFile = oldestFile || { oldest: Date(), count: 0 };
  files.forEach(function (file) {
    if (fs.statSync(dirPath + "/" + file).isDirectory()) {
      oldestFile = getAllDirFiles(dirPath + "/" + file, oldestFile);
    } else {
      let fileAge = fs.statSync(dirPath + "/" + file).birthtime;
      oldestFile.oldest = [fileAge, oldestFile.oldest].reduce((pre, cur) => {
        return pre < cur ? cur : pre;
      });
      oldestFile.count++;
    }
  });
  return oldestFile;
};

console.log(getAllDirFiles(__dirname + "\\test"));
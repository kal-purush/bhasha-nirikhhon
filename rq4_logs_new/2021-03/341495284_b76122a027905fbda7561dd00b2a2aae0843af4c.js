const fs = require('fs');
const {COPYFILE_EXCL} = fs.constants;
const {v4} = require('uuid');
const mysql = require('mysql2');

if (process.argv.length < 3) {
  console.error('请输入音乐文件路径！');
  return;
}

const filePath = process.argv[2];
let fileName = process.argv[2];

let lastFlag = filePath.lastIndexOf('\\');
if (lastFlag === -1) {
  lastFlag = filePath.lastIndexOf("/");
}

if (lastFlag !== -1) {
  fileName = filePath.substring(lastFlag + 1);
}
let fileExtName = fileName;
const fileExtNameIndex = fileName.lastIndexOf('.');
if (fileExtNameIndex !== -1) {
  fileExtName = fileName.substring(fileExtNameIndex + 1);
}

const newFileName = v4().toString().replace(/-/g, '');

const filePathDir = filePath.substring(0, lastFlag);

const lrcFilePath = filePathDir + '\\' + fileName.substring(0, fileExtNameIndex) + '.lrc';

const existMusicFile = fs.existsSync(filePath);
const existLrcFile = fs.existsSync(lrcFilePath);

console.log(`
filePath:${filePath}
fileName:${fileName}
newFileName:${newFileName}
fileExtName:${fileExtName}
filePathDir:${filePathDir}
lrcFilePath:${lrcFilePath}
existMusicFile:${existMusicFile}
existLrcFile:${existLrcFile}
`);

const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'yunshu_music'
});


const splitIndex = fileName.indexOf('-');
const title = fileName.substring(0, splitIndex).trim();
const artists = fileName.substring(splitIndex + 1, fileExtNameIndex).trim();

if (existMusicFile) {

  fs.copyFileSync(filePath, `F:\\music_yunshu\\${newFileName}`, COPYFILE_EXCL);
  if (existLrcFile) {
    fs.copyFileSync(lrcFilePath, `F:\\lyric_yunshu\\${newFileName}`, COPYFILE_EXCL);
  }

  let type;
  if (fileExtName === 'mp3') {
    type = 2;
  } else if (fileExtName === 'wav') {
    type = 3;
  } else if (fileExtName === 'aac') {
    type = 4;
  } else {
    type = 1;
  }

  const sql = `INSERT INTO music (music_id,lyric_id,name,singer,type,gmt_create,gmt_modified)
      VALUES ('${newFileName}','${newFileName}','${title}','${artists}',${type},NOW(),NOW())`;

  console.log(`Type: ${type} SQL: ${sql}`);

  connection.execute(sql);
  console.log('SUCCESS');
}
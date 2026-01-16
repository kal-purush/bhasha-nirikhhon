/**
 * Project Name : BunkatsuKun
 * 
 * @author      Keit-oDama
 * @date        Dec 12, 2020
 * @description ファイル操作ライブラリ
 * 
 */

'use strict';   // 処理を厳格にする
const fs = require('graceful-fs');      // ERROR:Too many open filesを回避できるfs / 参考(http://dotnsf.blog.jp/archives/1064268194.html)
const readline = require('readline');
const path = require('path');
const mainDirectory = path.dirname(process.argv[1]);

module.exports = {
    /**
     * @description ファイルを分割
     * @param  {String} filePath ファイルパス
     * @param  {Number} lineLimit 分割行数
     */
    splitFile: (filePath, lineLimit) => {
        // 分割したファイルを格納するフォルダを作成
        const directoryPrefix = 'Bunkatsu-';
        const fileName = path.basename(filePath);
        const directoryName = `${directoryPrefix}${fileName}`;
        if (fs.existsSync(`${mainDirectory}\\${directoryName}`)) {
            fs.rmdirSync(`${mainDirectory}\\${directoryName}`, { recursive: true });
        }
        fs.mkdirSync(`${mainDirectory}\\${directoryName}`);

        // 1行ずつ読み込み＆lineLimitだけ読み込んだら書き込み
        const rs = fs.createReadStream(filePath, 'utf8');
        const rl = readline.createInterface(rs, {});
        const extension = path.extname(fileName);
        const onlyFileName = path.basename(filePath, extension);
        let n = 0;
        let tmpN = 0;
        let dataBuf = '';
        rl.on('line', chunk => {
            dataBuf = `${dataBuf}${chunk}\n`;
            n++;
            if (n == lineLimit) {
                tmpN = tmpN + n;
                let ws = fs.createWriteStream(`${mainDirectory}\\${directoryName}\\${onlyFileName}-${tmpN/lineLimit}${extension}`, 'utf8');
                ws.write(dataBuf);
                n = 0;
                dataBuf = '';
            }
        });

        // 最終行に到達したら最後に溜まっているdataを吐き出して終了
        rl.on('close', () => {
            let ws = fs.createWriteStream(`${mainDirectory}\\${directoryName}\\${onlyFileName}-${(tmpN/lineLimit)+1}${extension}`, 'utf8');
            ws.write(dataBuf);
            n = 0;
            dataBuf = '';
            // console.log('end');
        });
    },
}
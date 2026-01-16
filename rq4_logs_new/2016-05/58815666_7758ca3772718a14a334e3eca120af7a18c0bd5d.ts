import * as stream from 'stream';
import * as fs from 'fs';
class LogStream extends stream.Transform {
  _transform(chunk, encoding, cb) {
    this.push(`${JSON.stringify(chunk)},\n`);
    cb();
  }
}
const pushStream = (filename: string) => {
  const writeStream = fs.createWriteStream(filename);
  const logStream = new LogStream({ objectMode: true });
  logStream
    .pipe(writeStream);
  return logStream.write.bind(logStream);
}

export default pushStream;

const write = pushStream(__dirname + '/hoge.txt');
Array.from({ length: 100 }, (i, key) => ({ key, name: `${key}man` })).forEach(i => write(i));
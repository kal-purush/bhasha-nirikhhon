import { parseXml, Document } from 'libxmljs';
import * as fs from 'fs';

function readFile(path: string): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    fs.readFile(path, undefined, (error, data) => {
      if (error) {
        reject(error);
      } else {
        resolve(data.toString());
      }
    });
  });
}

readFile('./src/declarative/View.xsd').then(xsdStr => {
  const xsd: Document = parseXml(xsdStr);
  console.log('Successfully parsed ./View.xsd');
}).catch(error => {
  console.error(error.toString());
});
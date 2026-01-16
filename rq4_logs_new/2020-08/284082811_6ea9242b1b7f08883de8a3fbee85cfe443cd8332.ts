import { parseXml, Document } from 'libxmljs';

import { readFile, writeFile } from '../FileHelpers';
import DocumentTranslator from '../translators/DocumentTranslator';

describe('Code generation', () => {
  it.skip('can translate a valid XML document into a text file', async () => {
    return readFile('./src/declarative/tests/TestValid.xml').then(xmlStr => {
      const xml = parseXml(xmlStr, { noblanks: true });
      const documentTranslator: DocumentTranslator = new DocumentTranslator('babylonjs', '../../index');
      const fileOutput: string = documentTranslator.translate(xml);
      return writeFile('./src/declarative/tests/Valid.ts', fileOutput);
    });
  });
});
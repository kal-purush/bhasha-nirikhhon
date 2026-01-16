import { parseXml, Document } from 'libxmljs';

import { readFile } from '../FileHelpers';

describe('View.xsd', () => {
  it('is a valid schema definition', async () => {
    return readFile('./src/declarative/View.xsd').then(xsdStr => {
      const xsd: Document = parseXml(xsdStr);
    }).catch(error => {
      fail(error);
    });
  });

  it('identifies a valid XML file', async () => {
    return Promise.all([
      readFile('./src/declarative/View.xsd'),
      readFile('./src/declarative/tests/TestValid.xml')
    ]).then(([xsdStr, xmlStr]) => {
      const xsd: Document = parseXml(xsdStr);
      const xml: Document = parseXml(xmlStr);
      const isValid: boolean = xml.validate(xsd);
      expect(isValid).toBe(true);
    }).catch(error => {
      fail(error);
    });
  });

  it('identifies an invalid XML file', async () => {
    return Promise.all([
      readFile('./src/declarative/View.xsd'),
      readFile('./src/declarative/tests/TestInvalid.xml')
    ]).then(([xsdStr, xmlStr]) => {
      const xsd: Document = parseXml(xsdStr);
      const xml: Document = parseXml(xmlStr);
      const isValid: boolean = xml.validate(xsd);
      expect(isValid).toBe(false);
      expect(xml.validationErrors.length).toBe(7);
    }).catch(error => {
      fail(error);
    });
  });
});
// import Conf from 'conf'
import chalk from 'chalk';
import xpath from 'xpath';
import dom from 'xmldom';
import fs from 'fs';

// const conf = new Conf()
const DomParser = dom.DOMParser;

const attachments = (filePath) => {
  fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
      // console.error(err);
      console.error(chalk.red.bold(`Could not read ${filePath}`));
      console.error(chalk.red(err));

      return;
    }

    console.log(chalk.green.bold(`Read ${filePath}`));
    // console.log(chalk.green(data));

    const doc = new DomParser().parseFromString(data);

    // eslint-disable-next-line max-len
    //   //*[local-name(.)='Invoice']/*[local-name(.)='cac:AdditionalDocumentReference']/*[local-name(.)='cac:Attachment']/*[local-name(.)='cbc:EmbeddedDocumentBinaryObject']
    let select = xpath.useNamespaces({
      cac: 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
      cbc: 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
      inv: 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2',
      cre: 'urn:oasis:names:specification:ubl:schema:xsd:CreditNote-2',
    });

    // var nodes = xpath.select("", doc)
    const nodesInvoice = select('/inv:Invoice/cac:AdditionalDocumentReference/cac:Attachment/cbc:EmbeddedDocumentBinaryObject', doc);
    // loop for each node
    nodesInvoice.forEach((node) => {
      // @filename="831018682_46923535_GGLE7.pdf"
      // @mimeCode="application/pdf"
      const filename = node.getAttribute('filename');
      const mimeCode = node.getAttribute('mimeCode');
      const content = node.textContent;
      if (mimeCode === 'application/pdf') {
        console.log(chalk.green.bold(`Saving attachment to ${filePath}_${filename}`));
        fs.writeFileSync(`${filePath}_${filename}`, content, 'base64');
      }
    });

    select = xpath.useNamespaces({
      cac: 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
      cbc: 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
      inv: 'urn:oasis:names:specification:ubl:schema:xsd:CreditNote-2',
    });

    const nodesCreditnote = select('/inv:CreditNote/cac:AdditionalDocumentReference/cac:Attachment/cbc:EmbeddedDocumentBinaryObject', doc);
    // loop for each node
    nodesCreditnote.forEach((node) => {
      // @filename="831018682_46923535_GGLE7.pdf"
      // @mimeCode="application/pdf"
      const filename = node.getAttribute('filename');
      const mimeCode = node.getAttribute('mimeCode');
      const content = node.textContent;
      if (mimeCode === 'application/pdf') {
        console.log(chalk.green.bold(`Saving attachment to ${filePath}_${filename}`));
        fs.writeFileSync(`${filePath}_${filename}`, content, 'base64');
      }
    });
    // console.log(chalk.green.bold(`${nodes}`))
  });
};

// export attachments;
export default attachments;
import { BlobServiceClient, StorageSharedKeyCredential } from '@azure/storage-blob';
import ExcelJS from 'exceljs';


// Configure Azure Storage with your account name, container name, and account key
const accountName = 'customersladata';
const containerName = 'new-tasks-collection';
const sasToken = '?sp=racwdli&st=2023-12-15T12:46:04Z&se=2024-11-30T20:46:04Z&sv=2022-11-02&sr=c&sig=%2Bwky7NQZW%2FcCKMHzu3yHRVN4jY6BNLdtqaSfSr6h2MY%3D';

// const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
const blobServiceClient = new BlobServiceClient(
    `https://${accountName}.blob.core.windows.net${sasToken}`
);

// const containerClient = blobServiceClient.getContainerClient(containerName);
export const writeToExcel = async (email, filePath) => {
  try {
    let workbook;
    try {
      // Try to read the existing workbook
      const existingBuffer = await downloadFromAzureBlobStorage(containerName, 'new-tasks-collection.xlsx');
      
      workbook = new ExcelJS.Workbook();
      await workbook.xlsx.load(existingBuffer);
    } catch (readError) {
      // If the file doesn't exist or cannot be read, create a new workbook
      workbook = new ExcelJS.Workbook();
    }

    // Get the first worksheet
    const sheet = workbook.getWorksheet(1) || workbook.addWorksheet('Sheet 1');

    // Find the first empty row
    const emptyRow = sheet.actualRowCount + 1;

    // Assuming the email is a string
    sheet.getCell(emptyRow, 1).value = email;

    // Save the workbook to a file
    await uploadToAzureBlobStorage(containerName, 'new-tasks-collection.xlsx', await workbook.xlsx.writeBuffer());
  } catch (writeError) {
    throw writeError;
  }
}

export const uploadToAzureBlobStorage = async (containerName, blobName, content) => {
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);

  const uploadResponse = await blockBlobClient.upload(content, content.length);
  console.log(`Upload block blob ${blobName} successfully`, uploadResponse.requestId);
}

async function downloadFromAzureBlobStorage(containerName, blobName) {
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blobClient = containerClient.getBlockBlobClient(blobName);

  const downloadBlockBlobResponse = await blobClient.download();
  const content = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody);

  return content;
}

async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on('data', (data) => {
      chunks.push(data instanceof Buffer ? data : Buffer.from(data));
    });
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on('error', reject);
  });
}
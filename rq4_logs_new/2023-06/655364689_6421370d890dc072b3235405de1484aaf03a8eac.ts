export class FileUploadValidator {
  static getFileExtension(fileName: string) {
    return fileName.split('.').pop();
  }
  static getFileName(fileName: string) {
    return fileName.split('.')[0];
  }
}
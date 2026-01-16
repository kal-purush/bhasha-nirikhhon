import { FileSystemService } from './FileSystemService';
import { join } from 'path';

describe('FileSystemService', () => {
  it(`should get list of files from specified direcotry`, async () => {
    const fsService = new FileSystemService();
    
    const files = ['FileSystemService.spec.ts', 'FileSystemService.ts', 'json-like-mock.js'];
    const list = await fsService.getFileNames('./src/services/file-system');

    expect(files).toEqual(list);
  });

  it('should get content of json like file as PJSO', async () => {
    const fsService = new FileSystemService();
    const mockObj = {
      "myTestJson": true,
      "nestedObject": {
        "int": 1,
        "string": "some string" 
      }
    };

    const mock = await fsService.getFileContentAsObj(join(__dirname, 'json-like-mock.js'));

    expect(mock).toBeInstanceOf(Object);
    expect(mock).toEqual(mockObj);
  });
});
import { FileListSanitizeStrategy } from './FileListSanitizeStrategy';
export declare class FileSystemService {
    private sStrategy;
    private backupSufix;
    constructor(sStrategy: FileListSanitizeStrategy);
    getFiles(path: string): Promise<Array<string>>;
    getFileName(path: string): string;
    getFileNames(path: string): Promise<Array<string>>;
    getFileContentAsObj(path: string): Promise<Object>;
    saveContentToFile(path: string, content: any, doBackup?: boolean): Promise<void>;
    getBackupPath(path: string): string;
}
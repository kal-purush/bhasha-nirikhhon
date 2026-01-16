export declare class CsvService {
    static separator: string;
    static getNumberOfLines(csvFilePath: string): Promise<number>;
    static getLineByIndex(csvFilePath: string, lineNumber: number): Promise<string>;
    static getLineByColumn(csvFilePath: string, columnIndex: number, columnData: string, caseSensitive?: boolean): Promise<string>;
    static getColumnByIndex(csvFilePath: string, columnIndex: number): Promise<string[]>;
    static getRandomLine(csvFilePath: string): Promise<string>;
}
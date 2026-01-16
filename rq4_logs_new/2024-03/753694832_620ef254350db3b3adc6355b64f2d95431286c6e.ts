import { parseCSV } from './csvreader';

test('valid CSV files', () => {
    expect(parseCSV('a,b,c\ne,f,g', undefined));
    expect(parseCSV('a,b,c\ne,f,g\n', undefined));
    expect(parseCSV('a,b,c\n\ne,f,g\n\n', undefined));
    expect(parseCSV('a,b,c\r\ne,f,g\r\n', undefined));
    expect(parseCSV('a,b,c\r\ne,,g\r\n', undefined));
    expect(parseCSV('a,b,c\r\n,,g\r\n', undefined));
    expect(parseCSV('a,b,c\r\n,,\r\n', undefined));
    expect(parseCSV('\n\n\r\n"a,b",b,c\r\nx,"y,t",z\r\n', undefined));
    expect(parseCSV('"a,b",b,c\r\nx,"y\n,,t",z\r\n', undefined));
    expect(parseCSV('a\ne', undefined));
    expect(parseCSV(`a"a"\ne`, undefined));
    expect(parseCSV(`"a","b"\ne,f`, undefined));
    expect(parseCSV(`"a",a"b"\ne,f`, undefined));
    expect(parseCSV(`,,,\na,b,c,d`, undefined));
    expect(parseCSV(`,"a\n"`, undefined));
    expect(parseCSV(`a,"a\n"\nx,"y\n"`, undefined));
    expect(parseCSV(`a,"a,\nb"`, undefined));
    expect(parseCSV(`a,"a\n,b"`, undefined));
    expect(parseCSV(`a,"a,b"`, undefined));
    expect(parseCSV(`a,b,c,d`, undefined));
});

test('invalid CSV files', () => {
    expect(() => parseCSV('a,b\nc,d,e,e,f', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc,d,e', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc,d,e,', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc,d,e,\n', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc,d,\ne,\n', undefined)).toThrow();
    expect(() => parseCSV('a,b\nc,d,\ne,"a"a\n', undefined)).toThrow();
});
import { parseGSUrl, GSUrl } from './gsutil';

describe('gsutil/parseGSUrl', () => {
  it.each([
    [
      'gs://bucket/path/to/file',
      {
        bucket: 'bucket',
        path: ['path', 'to'],
        filename: 'file',
        fullPath: 'path/to/file',
      },
    ],
    [
      'gs://bucket/path/file',
      {
        bucket: 'bucket',
        path: ['path'],
        filename: 'file',
        fullPath: 'path/file',
      },
    ],
    [
      'gs://bucket/file',
      {
        bucket: 'bucket',
        path: [],
        filename: 'file',
        fullPath: 'file',
      },
    ],
    [
      'gs://bucket/path/',
      {
        bucket: 'bucket',
        path: ['path'],
        fullPath: 'path/',
      },
    ],
  ])('should correctly parse "%s"', ((url: string, expected: GSUrl) => {
    expect(parseGSUrl(url)).toEqual({
      ...expected,
      url,
    });
  }) as any);

  it.each([['http://foo.bar'], ['gs://bucket'], ['gs://bucket/']])(
    'should detect invalid url: %s',
    (url) => {
      expect(parseGSUrl(url)).toBeNull();
    }
  );
});
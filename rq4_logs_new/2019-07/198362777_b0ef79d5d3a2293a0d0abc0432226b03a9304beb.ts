import * as crawler from '../../src/utils/crawl';

jest.mock('../../src/utils/crawl.ts');

test('is crawl method called', async () => {
  await crawler.crawl();
  expect(crawler.crawl).toHaveBeenCalled();
});
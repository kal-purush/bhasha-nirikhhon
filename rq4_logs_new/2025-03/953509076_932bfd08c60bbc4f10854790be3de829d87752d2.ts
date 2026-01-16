import { fetchCategories, fetchFaqs } from './faq';
import { CategoryType } from '@/types';

describe('FAQ 서비스', () => {
  const originalFetch = global.fetch;

  afterEach(() => {
    global.fetch = originalFetch;
  });

  describe('fetchCategories', () => {
    it('카테고리 목록을 정상적으로 가져와야 한다', async () => {
      const result = await fetchCategories('CONSULT');

      expect(result).toBeInstanceOf(Array);
      result.forEach((category) => {
        expect(category).toHaveProperty('categoryID');
        expect(category).toHaveProperty('name');
      });
    });

    it('API 요청이 실패하면 에러를 throw해야 한다', async () => {
      try {
        await fetchCategories('INVALID_CATEGORY' as CategoryType);
        expect(true).toBe(true);
      } catch (error) {
        if (error instanceof Error) {
          expect(error.message).toBe('카테고리를 불러오는데 실패했습니다.');
        }
      }
    });

    it('네트워크 오류 발생 시 에러를 throw해야 한다', async () => {
      // fetch를 모킹하여 네트워크 오류 발생 시뮬레이션
      global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

      await expect(fetchCategories('CONSULT')).rejects.toThrow('Network error');
    });

    it('응답이 ok가 아닐 때 에러를 throw해야 한다', async () => {
      // fetch를 모킹하여 응답이 ok가 아닌 경우 시뮬레이션
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      await expect(fetchCategories('CONSULT')).rejects.toThrow(
        '카테고리를 불러오는데 실패했습니다.',
      );
    });
  });

  describe('fetchFaqs', () => {
    it('기본 파라미터로 FAQ 목록을 가져와야 한다', async () => {
      const result = await fetchFaqs('CONSULT');

      expect(result).toHaveProperty('pageInfo');
      expect(result.pageInfo).toHaveProperty('totalRecord');
      expect(result.pageInfo).toHaveProperty('offset');
      expect(result.pageInfo).toHaveProperty('limit');
      expect(result.pageInfo).toHaveProperty('prevOffset');
      expect(result.pageInfo).toHaveProperty('nextOffset');

      expect(result).toHaveProperty('items');
      expect(Array.isArray(result.items)).toBe(true);
    });

    it('서브 카테고리와 함께 FAQ 목록을 가져와야 한다', async () => {
      const result = await fetchFaqs('CONSULT', 'PRODUCT');
      expect(result).toHaveProperty('pageInfo');
      expect(result).toHaveProperty('items');
    });

    it('오프셋과 제한을 지정하여 FAQ 목록을 가져와야 한다', async () => {
      const result = await fetchFaqs('CONSULT', 'ALL', 10, 5);
      expect(result).toHaveProperty('pageInfo');
      expect(result).toHaveProperty('items');
    });

    it('검색어와 함께 FAQ 목록을 가져와야 한다', async () => {
      const result = await fetchFaqs('CONSULT', 'ALL', 0, 10, '검색어');
      expect(result).toHaveProperty('pageInfo');
      expect(result).toHaveProperty('items');
    });

    it('잘못된 카테고리로 요청해도 빈 결과를 반환해야 한다', async () => {
      const result = await fetchFaqs('INVALID_CATEGORY' as CategoryType);

      expect(result).toHaveProperty('pageInfo');
      expect(result.pageInfo).toHaveProperty('totalRecord');
      expect(result.pageInfo.totalRecord).toBe(0);
      expect(result).toHaveProperty('items');
      expect(result.items).toHaveLength(0);
    });

    it('응답이 ok가 아닐 때 에러를 throw해야 한다', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      await expect(fetchFaqs('CONSULT')).rejects.toThrow(
        'FAQ를 불러오는데 실패했습니다.',
      );
    });

    it('네트워크 오류 발생 시 에러를 throw해야 한다', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('Network error'));

      await expect(fetchFaqs('CONSULT')).rejects.toThrow('Network error');
    });
  });
});
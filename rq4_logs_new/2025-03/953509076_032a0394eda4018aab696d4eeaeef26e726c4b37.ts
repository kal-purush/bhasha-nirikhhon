import { useFaqCategories, useFaqList } from './useFAQService';
import { fetchCategories, fetchFaqs } from './faq';
import { CategoryType, SubCategoryType } from '@/types';
import {
  useSuspenseQuery,
  useSuspenseInfiniteQuery,
} from '@tanstack/react-query';

vi.mock('@tanstack/react-query', () => ({
  useSuspenseQuery: vi.fn(),
  useSuspenseInfiniteQuery: vi.fn(),
}));

vi.mock('./faq', () => ({
  fetchCategories: vi.fn(),
  fetchFaqs: vi.fn(),
}));

describe('useFAQService', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('useFaqCategories', () => {
    it('올바른 queryKey와 queryFn으로 useSuspenseQuery를 호출해야 한다', () => {
      (useSuspenseQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue(
        {
          data: [{ categoryID: 'PRODUCT', name: '상품' }],
        },
      );

      const result = useFaqCategories('CONSULT');

      expect(useSuspenseQuery).toHaveBeenCalledWith({
        queryKey: expect.arrayContaining(['faq', 'categories', 'CONSULT']),
        queryFn: expect.any(Function),
      });

      const queryFnArg = (
        useSuspenseQuery as unknown as ReturnType<typeof vi.fn>
      ).mock.calls[0][0].queryFn;
      queryFnArg();

      expect(fetchCategories).toHaveBeenCalledWith('CONSULT');

      expect(result.data).toEqual([{ categoryID: 'PRODUCT', name: '상품' }]);
    });
  });

  describe('useFaqList', () => {
    it('올바른 인자로 useSuspenseInfiniteQuery를 호출해야 한다', () => {
      const mockData = {
        pages: [
          {
            pageInfo: {
              totalRecord: 30,
              offset: 0,
              limit: 10,
              prevOffset: 0,
              nextOffset: 10,
            },
            items: [
              {
                id: 1,
                question: 'Q1',
                answer: 'A1',
                categoryName: 'Test',
                subCategoryName: 'SubTest',
              },
            ],
          },
        ],
        pageParams: [0],
      };

      const mockFetchNextPage = vi.fn();
      const mockGetNextPageParam = vi.fn();

      (
        useSuspenseInfiniteQuery as unknown as ReturnType<typeof vi.fn>
      ).mockImplementation((options) => {
        if (options.getNextPageParam) {
          mockGetNextPageParam.mockImplementation(options.getNextPageParam);
        }

        return {
          data: mockData,
          fetchNextPage: mockFetchNextPage,
          hasNextPage: true,
        };
      });

      const params = {
        category: 'CONSULT' as CategoryType,
        subCategory: 'PRODUCT' as SubCategoryType,
        limit: 5,
        query: '검색어',
      };

      const result = useFaqList(params);

      expect(useSuspenseInfiniteQuery).toHaveBeenCalledWith(
        expect.objectContaining({
          queryKey: expect.arrayContaining([
            'faq',
            'list',
            'CONSULT',
            'PRODUCT',
            '검색어',
          ]),
          queryFn: expect.any(Function),
          initialPageParam: 0,
          getNextPageParam: expect.any(Function),
        }),
      );

      const queryFnArg = (
        useSuspenseInfiniteQuery as unknown as ReturnType<typeof vi.fn>
      ).mock.calls[0][0].queryFn;
      queryFnArg({ pageParam: 10 });

      expect(fetchFaqs).toHaveBeenCalledWith(
        'CONSULT',
        'PRODUCT',
        10,
        5,
        '검색어',
      );

      expect(result.data).toBe(mockData);
      expect(result.fetchNextPage).toBe(mockFetchNextPage);
      expect(result.hasNextPage).toBe(true);

      const lastPage = {
        pageInfo: {
          totalRecord: 30,
          offset: 0,
          limit: 10,
          prevOffset: 0,
          nextOffset: 10,
        },
        items: [],
      };

      const nextPageParam = mockGetNextPageParam(lastPage);
      expect(nextPageParam).toBe(10);

      const lastPageData = {
        pageInfo: {
          totalRecord: 10,
          offset: 0,
          limit: 10,
          prevOffset: 0,
          nextOffset: 10,
        },
        items: [],
      };

      const lastPageResult = mockGetNextPageParam(lastPageData);
      expect(lastPageResult).toBeUndefined();
    });
  });
});
import { Category, CategoryType, FaqResponse, SubCategoryType } from '@/types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:3000';
console.log(API_BASE_URL);
/**
 * 카테고리별 FAQ 카테고리 목록을 가져옵니다.
 * @param category 'CONSULT' 또는 'USAGE' 카테고리
 * @returns 카테고리 목록
 */
export async function fetchCategories(
  category: CategoryType,
): Promise<Category[]> {
  const response = await fetch(
    `${API_BASE_URL}/api/faq/category?tab=${category}`,
  );

  if (!response.ok) {
    throw new Error('카테고리를 불러오는데 실패했습니다.');
  }

  return response.json();
}

/**
 * FAQ 목록을 가져옵니다.
 * @param category 메인 카테고리 (CONSULT/USAGE)
 * @param subCategory 서브 카테고리
 * @param offset 페이지네이션 오프셋
 * @param limit 한 페이지당 항목 수
 * @param query 검색어 (선택사항)
 * @returns FAQ 응답 데이터
 */
export async function fetchFaqs(
  category: CategoryType,
  subCategory: SubCategoryType = 'ALL',
  offset: number = 0,
  limit: number = 10,
  query?: string,
): Promise<FaqResponse> {
  const queryParams = new URLSearchParams({
    tab: category,
    category: subCategory,
    offset: offset.toString(),
    limit: limit.toString(),
  });

  if (query) {
    queryParams.append('query', query);
  }

  const response = await fetch(
    `${API_BASE_URL}/api/faq?${queryParams.toString()}`,
  );

  if (!response.ok) {
    throw new Error('FAQ를 불러오는데 실패했습니다.');
  }

  return response.json();
}
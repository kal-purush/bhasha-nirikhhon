import { useState, useEffect } from 'react';
import { ReviewItemResponseDto } from '@/lib/types/review/ReviewItemResponseDto';
import { useSearchLogic } from './useSearchLogic';

/**
 * 검색 서비스 훅
 * 검색 결과를 가져오고 관리하는 로직을 제공합니다.
 * 
 * @param searchQuery 검색어
 * @param searchType 검색 유형 ('all', 'review', 'webtoon', 'user')
 * @param initialSort 초기 정렬 방식 (기본값: 'recommend')
 * @param limit 페이지당 결과 수 (기본값: 10)
 */
export function useSearchService(
  searchQuery: string,
  searchType: 'all' | 'review' | 'webtoon' | 'user',
  initialSort = 'recommend',
  limit?: number
) {
  // useSearchLogic 훅 사용
  const {
    items,
    isLoading,
    currentPage,
    totalPages,
    sortBy,
    goToNextPage,
    goToPrevPage,
    handleSortChange,
    hasMore,
    loadMore
  } = useSearchLogic(searchQuery, searchType, initialSort, limit);

  // 검색 결과 상태
  const [searchResults, setSearchResults] = useState<ReviewItemResponseDto[]>([]);

  // 검색 결과 업데이트
  useEffect(() => {
    setSearchResults(items);
  }, [items]);

  // 결과 반환
  return {
    searchResults,
    isLoading,
    currentPage,
    totalPages,
    sortBy,
    goToNextPage,
    goToPrevPage,
    handleSortChange,
    hasMore,
    loadMore
  };
} 
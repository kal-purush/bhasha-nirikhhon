import { useState, useCallback, useRef } from 'react';
import { ReviewItemResponseDto } from '@/lib/types/review/ReviewItemResponseDto';
import { getSearchParams, convertSortParam } from './searchMappings';
import { search } from '../api/search/search';

/**
 * 검색 데이터 가져오기 및 상태 관리를 위한 핵심 훅
 * @param searchQuery 검색어
 * @param searchType 검색 유형
 * @param initialSort 초기 정렬 방식
 * @param limit 페이지당 결과 수
 */
export const useSearchCore = (
  searchQuery: string,
  searchType: string,
  initialSort = 'recommend',
  limit = 10
) => {
  // 상태 관리
  const [items, setItems] = useState<ReviewItemResponseDto[]>([]);
  const [currentPage, setCurrentPage] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [sortBy, setSortBy] = useState(initialSort);
  const [hasMore, setHasMore] = useState(true);

  // 참조 변수 (불필요한 API 호출 방지)
  const isInitialLoadComplete = useRef(false);
  const lastSearchKey = useRef('');
  const currentSearchKey = `${searchQuery}:${searchType}:${sortBy}`;

  /**
   * 검색 결과를 가져오는 함수
   * @param page 페이지 번호
   * @param isLoadingMore 더 보기 로드 여부
   */
  const fetchResults = useCallback(
    async (page: number, isLoadingMore = false) => {
      // 검색 조건 체크
      if (!searchQuery.trim() || isLoading) return;

      // 캐시된 결과가 있고 동일한 검색인 경우 재요청 방지
      const shouldSkipRequest =
        isInitialLoadComplete.current &&
        lastSearchKey.current === currentSearchKey &&
        !isLoadingMore &&
        page === 0 &&
        items.length > 0;

      if (shouldSkipRequest) return;

      setIsLoading(true);

      try {
        // 검색 파라미터 설정
        const { searchTypeParam, filterParam } = getSearchParams(searchType);
        const effectiveSortBy = sortBy || 'recommend';
        const backendSortBy = convertSortParam(effectiveSortBy);

        // 정렬 변경 시 데이터 초기화
        if (lastSearchKey.current !== currentSearchKey && !isLoadingMore) {
          setItems([]);
        }

        // 검색 API 호출
        const data = await search(
          searchQuery,
          page,
          limit,
          searchTypeParam,
          backendSortBy,
          filterParam
        );

        // 검색 결과 처리
        if (data && Array.isArray(data.results)) {
          if (isLoadingMore) {
            // 중복 제거 후 기존 데이터에 추가
            const existingIds = new Set(items.map((item) => item.reviewId));
            const newItems = data.results.filter(
              (item) => !existingIds.has(item.reviewId)
            );
            setItems((prev) => [...prev, ...newItems]);
          } else {
            // 새 검색 결과로 교체
            setItems(data.results);
          }

          // 페이지 정보 업데이트
          setCurrentPage(data.currentPage || 0);
          setTotalPages(data.totalPages || 1);
          setHasMore(data.currentPage < data.totalPages - 1);
        } else {
          // 결과가 없는 경우
          if (!isLoadingMore) {
            setItems([]);
          }
          setHasMore(false);
        }

        // 검색 상태 업데이트
        isInitialLoadComplete.current = true;
        lastSearchKey.current = currentSearchKey;
      } catch (error) {
        console.error('검색 중 오류 발생:', error);

        if (!isLoadingMore) {
          setItems([]);
        }
        setHasMore(false);
        isInitialLoadComplete.current = true;
        lastSearchKey.current = currentSearchKey;
      } finally {
        setIsLoading(false);
      }
    },
    [searchQuery, sortBy, searchType, isLoading, items, currentSearchKey, limit]
  );

  return {
    items,
    setItems,
    currentPage,
    setCurrentPage,
    totalPages,
    isLoading,
    sortBy,
    setSortBy,
    hasMore,
    setHasMore,
    fetchResults,
    lastSearchKey,
    currentSearchKey,
    isInitialLoadComplete,
  };
};
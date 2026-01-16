import { useState, useEffect, useCallback, useRef } from 'react';
import useSearch from '@/lib/api/search/search';
import { ReviewItemResponseDto } from '@/lib/types/review/ReviewItemResponseDto';

interface SearchResult<T> {
  items: T[];
  isLoading: boolean;
  currentPage: number;
  totalPages: number;
  sortBy: string;
  goToNextPage: () => void;
  goToPrevPage: () => void;
  handleSortChange: (value: string) => void;
  hasMore: boolean;
  loadMore: () => void;
}

export function useSearchLogic(
  searchQuery: string, 
  searchType: string, 
  initialSort = 'recommend',
  limit?: number
): SearchResult<ReviewItemResponseDto> {
  const [items, setItems] = useState<ReviewItemResponseDto[]>([]);
  const [currentPage, setCurrentPage] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [sortBy, setSortBy] = useState(initialSort);
  const [hasMore, setHasMore] = useState(true);
  const { search } = useSearch();
  
  // 검색 요청 방지를 위한 참조 변수
  const isInitialLoadComplete = useRef(false);
  const lastSearchKey = useRef('');
  const currentSearchKey = `${searchQuery}:${searchType}:${sortBy}`;

  // 검색 실행 함수
  const fetchResults = useCallback(async (page: number, isLoadingMore = false) => {
    // 이미 로딩 중이거나 검색어가 없는 경우 건너뛰기
    if (!searchQuery.trim() || isLoading) return;
    
    // 초기 로딩이 이미 완료되었고, 같은 검색 요청이면서 추가 로드가 아닌 경우 건너뛰기
    if (isInitialLoadComplete.current && 
        lastSearchKey.current === currentSearchKey && 
        !isLoadingMore && 
        page === 0 && 
        items.length > 0) {
      return;
    }
    
    setIsLoading(true);
    
    try {
      // 백엔드 API에 맞게 파라미터 설정
      let searchTypeParam;
      let filterParam;
      
      switch(searchType) {
        case 'review':
          searchTypeParam = 'reviewContent';
          filterParam = 'review';
          break;
        case 'user':
          searchTypeParam = 'nickname';
          filterParam = 'user';
          break;
        case 'webtoon':
          searchTypeParam = 'webtoonName';
          filterParam = 'webtoon';
          break;
        case 'all':
          // 전체 검색 시 searchType을 지정하지 않고 필터만 'all'로 설정
          searchTypeParam = undefined;
          filterParam = 'all';
          break;
        // 혹시 URL 파라미터가 그대로 전달될 경우를 대비한 호환성 코드
        case 'userNickname':
          searchTypeParam = 'nickname';
          filterParam = 'user';
          break;
        case 'webtoonName':
          searchTypeParam = 'webtoonName';
          filterParam = 'webtoon';
          break;
        default:
          searchTypeParam = undefined;
          filterParam = 'all';
      }
      
      // 디버깅용 로그 (필요시 삭제 가능)
      console.log('검색 API 호출:', {
        searchQuery, 
        page,
        sortBy, 
        searchType,
        searchTypeParam, 
        filterParam,
        isLoadingMore,
        isInitialLoadComplete: isInitialLoadComplete.current
      });
      
      const data = await search(
        searchQuery,
        page,
        limit || 10,
        searchTypeParam,
        sortBy,
        filterParam
      );
      
      if (data && Array.isArray(data.results)) {
        if (isLoadingMore) {
          // 중복 데이터 방지를 위한 검사
          const existingIds = new Set(items.map(item => item.reviewId));
          const newItems = data.results.filter(item => !existingIds.has(item.reviewId));
          
          // 기존 데이터에 새로운 데이터 추가
          setItems(prev => [...prev, ...newItems]);
        } else {
          // 새로운 검색 결과로 전체 교체
          setItems(data.results);
        }
        
        setCurrentPage(data.currentPage || 0);
        setTotalPages(data.totalPages || 1);
        setHasMore(data.currentPage < data.totalPages - 1);
        
        // 검색 완료 표시
        isInitialLoadComplete.current = true;
        lastSearchKey.current = currentSearchKey;
      } else {
        if (!isLoadingMore) {
          setItems([]);
        }
        setHasMore(false);
        isInitialLoadComplete.current = true;
        lastSearchKey.current = currentSearchKey;
      }
    } catch (error: any) {
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
  }, [searchQuery, sortBy, searchType, search, limit, isLoading, items, currentSearchKey]);

  // 검색어 또는 정렬 방식 변경 시 초기 데이터 로드
  useEffect(() => {
    // 검색어가 변경되었을 때만 초기화
    if (lastSearchKey.current !== currentSearchKey) {
      isInitialLoadComplete.current = false;
      setCurrentPage(0);
      
      if (searchQuery) {
        fetchResults(0, false);
      } else {
        setItems([]);
        setHasMore(false);
        isInitialLoadComplete.current = true;
        lastSearchKey.current = currentSearchKey;
      }
    }
  }, [searchQuery, sortBy, searchType, fetchResults, currentSearchKey]);

  // 더 많은 결과 로드 (더 보기 버튼 클릭 시)
  const loadMore = useCallback(() => {
    if (isLoading || !hasMore) return;
    
    const nextPage = currentPage + 1;
    if (nextPage < totalPages) {
      fetchResults(nextPage, true);
    }
  }, [currentPage, totalPages, isLoading, hasMore, fetchResults]);

  const goToNextPage = () => {
    if (currentPage < totalPages - 1) {
      setCurrentPage(prev => prev + 1);
    }
  };

  const goToPrevPage = () => {
    if (currentPage > 0) {
      setCurrentPage(prev => prev - 1);
    }
  };

  const handleSortChange = (value: string) => {
    console.log('정렬 변경 시도:', { 
      이전정렬: sortBy, 
      새정렬: value, 
      검색유형: searchType,
      검색어: searchQuery
    });
    
    if (sortBy !== value) {
      // 정렬 상태 업데이트
      setSortBy(value);
      
      // 강제로 새로운 검색을 트리거하기 위해 lastSearchKey 초기화
      lastSearchKey.current = '';
      
      // 페이지 초기화
      setCurrentPage(0);
      
      // 강제로 새 검색 실행
      setTimeout(() => {
        fetchResults(0, false);
      }, 0);
      
      // 디버깅용: 정렬 변경 후 API 호출될 때 상태 기록
      console.log('정렬 변경 완료 - 강제 API 호출 예정:', {
        변경된정렬: value,
        검색유형: searchType,
        페이지: 0
      });
    }
  };

  return {
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
  };
} 
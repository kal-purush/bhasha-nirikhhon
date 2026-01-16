import axios from 'axios';
import { SearchResponseDto } from '@/lib/types/search/SearchResponseDto';
import { convertSortParam } from '@/lib/service/search/searchMappings';
import { SearchSuggestionDto } from '@/lib/types/search/SearchSuggestionDto';
import API_BASE_URL from '@/lib/utils/apiConfig';

/**
 * 검색 API를 호출하는 함수
 * @param keyword 검색어
 * @param page 페이지 번호
 * @param size 페이지당 결과 수
 * @param searchType 검색 타입
 * @param sortBy 정렬 방식
 * @param filter 필터
 */
export const search = async (
  keyword: string,
  page: number = 0,
  size: number = 10,
  searchType?: string,
  sortBy: string = 'recommend',
  filter: string = 'all'
): Promise<SearchResponseDto | null> => {
  try {
    // 프론트엔드 정렬 값을 백엔드 API 파라미터에 맞게 변환
    const sortByForBackend = convertSortParam(sortBy);
    
    const response = await axios.get<SearchResponseDto>(`${API_BASE_URL}/search`, {
      params: {
        keyword,
        page,
        size,
        searchType,
        sortBy: sortByForBackend,
        filter
      },
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      timeout: 10000
    });
    
    if (!response.data) {
      return null;
    }
    
    if (!response.data.results) {
      response.data.results = [];
    }
    
    return response.data;
  } catch (error) {
    console.error('검색 API 호출 중 오류 발생:', error);
    
    // 백엔드 연결 실패 시 기본 응답 제공
    return {
      keyword: keyword,
      results: [],
      currentPage: 0,
      totalPages: 0,
      totalElements: 0
    };
  }
};

/**
 * 추천수 기준으로 정렬된 검색을 수행하는 함수
 */
export const searchByRecommendations = async (
  keyword: string,
  page: number = 0,
  size: number = 10,
  searchType?: string,
  filter: string = 'all'
): Promise<SearchResponseDto | null> => {
  try {
    return await search(keyword, page, size, searchType, 'recommend', filter);
  } catch (error) {
    console.error('추천순 검색 중 오류 발생:', error);
    return null;
  }
};

/**
 * 최신순으로 정렬된 검색을 수행하는 함수
 */
export const searchByRecent = async (
  keyword: string,
  page: number = 0,
  size: number = 10,
  searchType?: string,
  filter: string = 'all'
): Promise<SearchResponseDto | null> => {
  try {
    return await search(keyword, page, size, searchType, 'recent', filter);
  } catch (error) {
    console.error('최신순 검색 중 오류 발생:', error);
    return null;
  }
};

/**
 * 조회수 기준으로 정렬된 검색을 수행하는 함수
 */
export const searchByViewCount = async (
  keyword: string,
  page: number = 0,
  size: number = 10,
  searchType?: string,
  filter: string = 'all'
): Promise<SearchResponseDto | null> => {
  try {
    return await search(keyword, page, size, searchType, 'viewCount', filter);
  } catch (error) {
    console.error('조회수순 검색 중 오류 발생:', error);
    return null;
  }
};

/**
 * 자동완성 제안을 가져오는 함수
 */
export const getSearchSuggestions = async (
  prefix: string,
  suggestionType?: string,
  sortBy: string = 'recommend',
  minMatchScore: number = 0.5,
  limit: number = 7
): Promise<SearchSuggestionDto | null> => {
  try {
    const response = await axios.get<SearchSuggestionDto>(`${API_BASE_URL}/search/suggestions`, {
      params: {
        prefix,
        suggestionType,
        sortBy,
        minMatchScore,
        limit
      },
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      timeout: 10000
    });
    return response.data;
  } catch (error) {
    console.error('자동완성 API 호출 중 오류 발생:', error);
    return null;
  }
};

/**
 * 인기 검색어 목록을 가져오는 함수
 */
export const getPopularSearchTerms = async (
  minScore?: number,
  limit: number = 10,
  recentDays?: number
): Promise<SearchSuggestionDto | null> => {
  try {
    const response = await axios.get<SearchSuggestionDto>(`${API_BASE_URL}/search/popular`, {
      params: {
        minScore,
        limit,
        recentDays
      },
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      timeout: 10000
    });
    return response.data;
  } catch (error) {
    console.error('인기 검색어 API 호출 중 오류 발생:', error);
    return null;
  }
};

/**
 * 검색 관련 캐시를 삭제하는 함수
 * @returns 성공 여부
 */
export const clearSearchCache = async (): Promise<boolean> => {
  try {
    const response = await axios.post<{ message: string }>(`${API_BASE_URL}/search/clear-cache`);
    console.log('검색 기록 삭제 완료:', response.data.message);
    return true;
  } catch (error) {
    console.error('검색 기록 삭제 중 오류 발생:', error);
    return false;
  }
}; 
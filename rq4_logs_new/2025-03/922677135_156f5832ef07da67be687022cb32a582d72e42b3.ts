import { useState, useEffect } from 'react';
import { getSearchSuggestions } from '@/lib/api/search';

interface UseSearchSuggestionsProps {
  searchText: string;
  debounceTime?: number;
  suggestionType?: string;
  sortBy?: string;
}

interface UseSearchSuggestionsResult {
  suggestions: string[];
  isLoading: boolean;
  error: Error | null;
}

/**
 * 검색어 자동완성 API를 호출하는 커스텀 훅
 */
export const useSearchSuggestions = ({
  searchText,
  debounceTime = 300,
  suggestionType,
  sortBy = 'recommend'
}: UseSearchSuggestionsProps): UseSearchSuggestionsResult => {
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    // 검색어가 없으면 추천 목록 비우기
    if (!searchText || searchText.length < 2) {
      setSuggestions([]);
      return;
    }

    // 디바운스 처리를 위한 타이머
    const timer = setTimeout(async () => {
      try {
        setIsLoading(true);
        const response = await getSearchSuggestions(searchText, suggestionType, sortBy);
        
        if (response) {
          setSuggestions(response.suggestions);
        } else {
          setSuggestions([]);
        }
        
        setError(null);
      } catch (err) {
        console.error('자동완성 API 호출 중 오류 발생:', err);
        setSuggestions([]);
        setError(err instanceof Error ? err : new Error('자동완성 데이터를 가져오는 중 오류가 발생했습니다.'));
      } finally {
        setIsLoading(false);
      }
    }, debounceTime);

    // 컴포넌트 언마운트 또는 searchText 변경 시 타이머 정리
    return () => clearTimeout(timer);
  }, [searchText, debounceTime, suggestionType, sortBy]);

  return { suggestions, isLoading, error };
};
import { useEffect, useState } from "react";
import { FetchPost } from "../_interfaces/fetchPostInterface";

export const useSearchResults = (query: string | string[] | undefined) => {
  const [results, setResults] = useState<FetchPost[]>([]);

  useEffect(() => {
    if (query) {
      fetch(`/api/search/keyword?query=${query}`)
        .then((res) => res.json())
        .then((data) => {
          console.log("Fetched data:", data); // デバッグ用に追加
          setResults(data);
        })
        .catch((err) => console.error("Error fetching search results:", err));
    }
  }, [query]);

  return { results };
};
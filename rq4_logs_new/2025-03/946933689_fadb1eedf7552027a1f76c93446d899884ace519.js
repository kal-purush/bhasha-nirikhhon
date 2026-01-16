import { useState, useEffect, useCallback } from "react";
import axios from "axios";

const API_URL = "http://localhost:5000/api/news";

const useNewsInfiniteScroll = () => {
  const [news, setNews] = useState([]); // Store news items
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);

  const fetchNews = useCallback(async () => {
    if (!hasMore || loading) return; // ✅ Stop fetching if no more data

    setLoading(true);
    try {
      console.log("Fetching news for page:", page);
      const response = await axios.get(`${API_URL}?page=${page}&limit=3`);
      const newNews = response.data.data;

      if (newNews.length === 0) {
        setHasMore(false); // ✅ Stop fetching if no more data
        return;
      }

      setNews((prevNews) => {
        const existingIds = new Set(prevNews.map((n) => n._id)); // ✅ Track existing news IDs
        const uniqueNews = newNews.filter((n) => !existingIds.has(n._id)); // ✅ Add only new items
        return [...prevNews, ...uniqueNews]; // ✅ Append only unique news
      });

      setPage((prevPage) => prevPage + 1);
      setHasMore(response.data.hasMore);
    } catch (error) {
      console.error("Error fetching news:", error);
    }
    setLoading(false);
  }, [page, hasMore, loading]);

  useEffect(() => {
    fetchNews(); // ✅ Ensure initial load starts from page 1
  }, []);

  useEffect(() => {
    const handleScroll = () => {
      if (
        window.innerHeight + window.scrollY >=
          document.body.offsetHeight - 100 &&
        hasMore &&
        !loading
      ) {
        fetchNews();
      }
    };

    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, [hasMore, loading, fetchNews]);

  return { news, loading, hasMore };
};

export default useNewsInfiniteScroll;
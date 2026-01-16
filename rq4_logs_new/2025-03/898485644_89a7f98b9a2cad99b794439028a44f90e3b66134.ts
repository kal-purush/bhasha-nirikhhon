import { useEffect, useState, useCallback } from "react";
import { useScary } from "@/app/_hooks/useScary";
import useFetchDetail from "@/app/_hooks/useFetchDetail";

export const usePostDetail = (postId: string) => {
  const { toggleScary, checkScaryStatus } = useScary();
  const { postDetail, refetch } = useFetchDetail(postId);

  const [scary, setScary] = useState(0);
  const [isScary, setIsScary] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const post = postDetail.length > 0 ? postDetail[0] : null;

  const checkStatus = useCallback(async () => {
    if (!post) return;
    const status = await checkScaryStatus(post.id);
    setIsScary(status);
  }, [post, checkScaryStatus]);

  useEffect(() => {
    if (post) {
      setScary(post.scaryCount);
      checkStatus();
    }
  }, [post, checkStatus]);

  const handleIconClick = async (event: React.MouseEvent) => {
    event.preventDefault();
    if (isLoading || !post) return;
    setIsLoading(true);
    try {
      const result = await toggleScary(post.id);
      setScary(result.scaryCount);
      setIsScary(result.isScary);
      refetch();
    } catch (err) {
      console.error(
        "Failed to update scary status:",
        err instanceof Error ? err.message : err
      );
    } finally {
      setIsLoading(false);
    }
  };

  return { post, scary, isScary, isLoading, handleIconClick };
};
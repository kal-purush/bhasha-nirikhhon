import { useState } from "react";

export function useScary() {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const addScary = async (postId: string) => {
    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch(`/api/post/${postId}/scary`, {
        method: "PATCH",
      });
      console.log("API Response:", res);

      if (!res.ok) {
        const errorData = await res.json();
        console.error("API Error:", errorData);
        throw new Error(errorData.error || "Failed to update scary count");
      }

      const updatedPost = await res.json();
      return updatedPost.scaryCount;
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred");
      throw err;
    } finally {
      setIsLoading(false);
    }
  };

  return { addScary, isLoading, error };
}
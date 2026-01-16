export async function fetchReviewCount() {
  try {
    const res = await fetch("https://api.reeltalk.com/reviews/count"); // ✅ API 주소에 맞게 수정
    if (!res.ok) throw new Error("Failed to fetch review count");
    const data = await res.json();
    return data.count;
  } catch (error) {
    console.error("Error fetching review count:", error);
    return 0;
  }
}
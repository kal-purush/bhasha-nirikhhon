import { createClient } from "@/lib/supabase/server";
import { GetComment } from "@/model/comment";
import { parseDate } from "@/util/date";

export async function getCommentList(postId: string): Promise<GetComment[] | null> {
  const supabase = createClient();

  const { data, error } = await supabase
    .from("comments")
    .select()
    .eq("postId", postId)
    .order("createdAt", { ascending: false });

  if (data === null) {
    return null;
  }

  const comments = data.map((comment) => ({ ...comment, createdAt: parseDate(comment.createdAt) }));

  return comments;
}
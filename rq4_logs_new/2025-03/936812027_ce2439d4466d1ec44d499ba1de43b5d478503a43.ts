import { defineQuery } from 'groq';
import { sanityFetch } from '../live';

export async function searchPosts(term: string, type: string) {
  const searchPostsQuery = defineQuery(`*[_type == "post" && (
    title match $term + "*" ||
    description match $term + "*" ||
    category->name match $term + "*"
  ) &&
  ($type == "all" || type == $type)] {
    ...,
    "slug": slug.current,
    "category": category->{...}
  }`);

  const result = await sanityFetch({
    query: searchPostsQuery,
    params: { term, type },
  });

  return result.data || [];
}
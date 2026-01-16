const getFilteredProducts = async (
  pageParam: string,
  query: string,
  category: string
) => {
  const resp = await fetch(`/api/productsAPI?pageParam=${pageParam}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: query,
      category: category,
    }),
  });
  const response = await resp.json();
  return response;
};

const getRawProducts = async (pageParam: string) => {
  const resp = await fetch(`/api/productsAPI?pageParam=${pageParam}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  });
  const response = await resp.json();
  return response;
};

export const handleFetchProducts = async ({
  queryKey,
  pageParam,
}: {
  queryKey: string[];
  pageParam: string;
}) => {
  // VERIFY: queryKey is an array of strings
  const query = queryKey[1];
  const category = queryKey[2];
  if ((query && query !== "") || (category && category !== "0")) {
    return getFilteredProducts(pageParam, query, category);
  } else {
    return getRawProducts(pageParam);
  }
};
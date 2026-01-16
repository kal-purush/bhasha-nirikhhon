export const fetcher = async (url: string, token?: string) => {
  const response = await fetch(url, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });

  if (!response.ok) throw new Error('failed to fetch');

  return response.json();
};
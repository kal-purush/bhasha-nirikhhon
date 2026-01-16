export const getProductsList = async () => {
  const apiUrl = `https://p7jb1wjlz4.execute-api.us-east-1.amazonaws.com/dev/products`;
  const response = await fetch(apiUrl);
  const data = await response.json();
  return data;
}
import { headers, baseUrl } from '..';

export async function fetchExpensesByDateRange(
  startDate: Date,
  endDate: Date,
  token?: string,
): Promise<any> {
  const resp = await fetch(`${baseUrl}/expenses?startDate=${startDate}&endDate=${endDate}`, {
    method: 'GET',
    headers: { ...headers, Authorization: `Bearer ${token}` },
  });

  return resp.json();
}
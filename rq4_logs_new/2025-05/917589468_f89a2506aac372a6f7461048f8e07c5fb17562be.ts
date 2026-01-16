import { Address } from 'viem';

const API = 'https://claude-service-api.vercel.app';
const API_LOCAL = 'http://localhost:8008';

export const getUserProof = async (address: Address) => {
  const res = await fetch(`${API_LOCAL}/proof/${address}`, {
    method: 'GET',
    headers: {
      'X-API-Key': import.meta.env.VITE_API_KEY,
    },
  });

  const json = await res.json();

  if (!json.success) {
    console.error('Error fetching proof:', json.error);
    return;
  }

  console.log('json.data', json.data.proof);

  return json.data.proof;
};
import { useState, useEffect } from 'react';
import axios, { AxiosError } from 'axios';

// Typing for the response and errors
interface DecodedToken {
  exp: number;
}

interface TokenResponse {
  token: string | null;
}

interface UseTokenReturn {
  token: string | null;
  error: string | null;
  fetchTokenFromDatabase: (user_id: string) => Promise<string | null>;
}
export const useToken = (): UseTokenReturn => {
  const [token, setToken] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fetchTokenFromDatabase = async (user_id: string): Promise<string | null> => {
    try {
      console.log("Fetching token for userId:", user_id);
      const response = await axios.get<TokenResponse>(`http://localhost:3002/token/${user_id}`);

      if (response.data.token) {
        console.log("Token found:", response.data.token);
        setToken(response.data.token); // This triggers re-render with updated token
        return response.data.token;
      } else {
        console.error("No token found for userId:", user_id);
        return null;
      }
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error("Axios error while fetching token:", error.response?.data);
      } else {
        console.error("Unexpected error while fetching token:", error);
      }
      return null;
    }
  };

  return { token, error, fetchTokenFromDatabase };
};
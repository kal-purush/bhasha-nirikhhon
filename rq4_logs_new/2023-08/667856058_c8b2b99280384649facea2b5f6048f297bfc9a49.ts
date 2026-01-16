// Hooks/useAuth.ts
import { useState, useEffect } from 'react';
import axios from 'axios';
import api from '../configs/api';

export function useAuth() {
  const initialUserData = JSON.parse(
    localStorage.getItem('userData') || 'null',
  );
  const initialGuestState = JSON.parse(
    localStorage.getItem('guestState') || 'false',
  );
  const [user, setUser] = useState<string | null>(initialUserData);
  const [guest, setGuestState] = useState(initialGuestState);

  useEffect(() => {
    // Save user data to local storage whenever user state changes
    if (user) {
      localStorage.setItem('userData', JSON.stringify(user));
    } else {
      localStorage.removeItem('userData');
    }
  }, [user]);

  useEffect(() => {
    // Save guest state to local storage whenever guest state changes
    if (guest) {
      localStorage.setItem('guestState', JSON.stringify(guest));
    } else {
      localStorage.removeItem('guestState');
    }
  }, [guest]);

  const login = async (email: string, password: string) => {
    try {
      const response = await axios.post(
        `${api.baseUrl}/buyers/login`,
        { email, password },
        {
          headers: {
            'Content-Type': 'application/json',
          },
        },
      );

      if (response.status === 200) {
        const userData = response.data;
        setUser(userData);
        setGuestState(false); // Reset guest state on successful login
        return response.status;
      } else {
        return response.status;
      }
    } catch (error) {
      throw new Error('Error during login.');
    }
  };

  const signup = async (email: string, password: string) => {
    try {
      const response = await axios.post(
        `${api.baseUrl}/buyers/signup`,
        { email, password },
        {
          headers: {
            'Content-Type': 'application/json',
          },
        },
      );

      if (response.status === 201) {
        const userData = response.data;
        setUser(userData);
        setGuestState(false); // Reset guest state on successful signup
        return response.status;
      } else {
        return response.status;
      }
    } catch (error) {
      throw new Error('Error during signup.');
    }
  };

  const logout = () => {
    setUser(null);
    setGuestState(false);
    localStorage.removeItem('userData');
    localStorage.removeItem('guestState');
  };

  const setGuest = (guestValue: boolean) => {
    setGuestState(guestValue);
  };

  return { user, guest, login, signup, logout, setGuest };
}
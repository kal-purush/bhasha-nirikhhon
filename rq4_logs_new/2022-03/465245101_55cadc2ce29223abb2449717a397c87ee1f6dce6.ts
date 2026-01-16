import React from 'react';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { fetchJson } from '../lib/api';
import { User } from '../lib/user';

const USER_QUERY_KEY = 'user';

interface SignInVariables {
  email: string;
  password: string;
}

interface UseSignInResult {
  signIn: (email: string, password: string) => Promise<boolean>;
  signInError: boolean;
  signInLoading: boolean;
}

export function useSignIn(): UseSignInResult {
  const queryClient = useQueryClient();
  const mutation = useMutation<User, Error, SignInVariables>(
    ({ email, password }) =>
      fetchJson('/api/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      })
  );
  return {
    signIn: async (email: string, password: string) => {
      try {
        const user = await mutation.mutateAsync({ email, password });
        queryClient.setQueryData(USER_QUERY_KEY, user);
        return true;
      } catch (err) {
        return false;
      }
    },
    signInError: mutation.isError,
    signInLoading: mutation.isLoading,
  };
}
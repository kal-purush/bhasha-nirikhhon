import { useSession } from 'next-auth/react';

export function useUser() {
  const { data: session} = useSession();
  return {
    isUser: session?.user?.role === 'USER',
  };
}
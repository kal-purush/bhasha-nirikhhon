import { useQuery } from '@tanstack/react-query';

export const useUsers = () => {
  const users = useQuery({
    queryKey: ['users'],
    queryFn: async () => {
      const res = await fetch('http://localhost:3000/trpc/user.getUsers');
      return await res.json();
    },
  });
  return users;
};
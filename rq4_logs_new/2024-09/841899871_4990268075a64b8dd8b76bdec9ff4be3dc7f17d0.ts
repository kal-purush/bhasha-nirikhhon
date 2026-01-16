import { checkAuthenticated } from '@/lib/user';
import usersService, { userQueryKeys } from '@/services/users';
import { useQuery } from 'react-query';

const useCurrentUser = () => {
  const isAuthenticated = checkAuthenticated();
  const { data, isLoading } = useQuery(
    userQueryKeys.getUser,
    usersService.getUser,
    {
      enabled: isAuthenticated,
      refetchOnWindowFocus: false,
    }
  );

  const user = data ?? null;

  return { user, isLoading };
};

export default useCurrentUser;
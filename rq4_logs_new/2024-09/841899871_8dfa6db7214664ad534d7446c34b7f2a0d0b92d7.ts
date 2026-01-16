import { AppRoute } from '@/router/constant';
import usersService from '@/services/users';
import { useMutation } from 'react-query';
import { useNavigate } from 'react-router-dom';

const useLogin = () => {
  const navigate = useNavigate();

  const login = useMutation({
    mutationFn: usersService.login,
    onSuccess: (response) => {
      localStorage.setItem('token', response.jwt);
      if (!response.user.emailVerified) {
        navigate(AppRoute.VerifyEmail);
      } else {
        navigate(AppRoute.Home);
      }
    },
  });

  return login;
};

export default useLogin;
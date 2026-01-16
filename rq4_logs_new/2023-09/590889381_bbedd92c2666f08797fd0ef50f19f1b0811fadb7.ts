import { useToast } from '@chakra-ui/react';

export const toastSummary = () => {
  const toast = useToast();

  const loginToast = () => {
    return toast({
      title: 'Successful login',
      description: 'Welcome to Gamers Board',
      position: 'top',
      status: 'success',
      duration: 7000,
      isClosable: true,
    });
  };

  const errorLoginToast = () => {
    return toast({
      title: 'Login failed',
      description: 'Password or Email is incorrect.',
      position: 'top',
      status: 'error',
      duration: 9000,
      isClosable: true,
    });
  };

  const successCreateAcountToast = () => {
    return toast({
      title: 'Account created.',
      description: "We've created your account for you.",
      status: 'success',
      duration: 9000,
      isClosable: true,
    });
  };

  return { loginToast, errorLoginToast, successCreateAcountToast };
};
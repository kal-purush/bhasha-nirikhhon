import { IRouter } from '@/types/router.type';

export const getSession = () => {
  try {
    // Handle get session logic here
    return true;
  } catch (error) {
    console.error('Error getting session:', error);
    return null;
  }
};

export const isInvalidRole = (roles?: IRouter['roles'], userRole?: string) => {
  if (!roles?.length || !userRole) {
    return false;
  }

  if (roles.some((role) => role === userRole)) {
    return false;
  }

  return true;
};
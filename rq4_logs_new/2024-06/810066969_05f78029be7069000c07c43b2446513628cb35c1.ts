import { useCallback, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { useAppContext } from '../context/AppContext';

export const useHandleMenuItemClick = () => {
  const { setSelectedNavItem, setSelectedMenu } = useAppContext();
  const location = useLocation();

  const handleMenuItemClick = useCallback(
    (page: string) => {
      setSelectedNavItem(page);
      setSelectedMenu(false);
    },
    [setSelectedNavItem, setSelectedMenu],
  );

  useEffect(() => {
    const pathname = location.pathname;
    const page =
      pathname === '/' ? 'Home' : pathname.slice(1).charAt(0).toUpperCase() + pathname.slice(2);
    setSelectedNavItem(page);
  }, [location, setSelectedNavItem]);

  return handleMenuItemClick;
};
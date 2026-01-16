import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

const getPageByPath = (path: string) => {
  const page = path.split('/')[1];

  if (page === '') {
    return 'home';
  }
  if (page === 'pointhistory') {
    return 'pointhistory';
  }
};

const useTransitionSelect = () => {
  const location = useLocation();
  const [transition, setTransition] = useState('');

  useEffect(() => {
    const previousPath = sessionStorage.getItem('path')!;
    const previousPage = getPageByPath(previousPath);
    const currentPage = getPageByPath(location.pathname);

    let newTransition = '';

    console.log('currentPage', currentPage);
    console.log('prevPage', previousPage);

    if (currentPage === 'home') {
      if (previousPage === 'pointhistory') {
        newTransition = 'slide-left';
      } else {
        newTransition = '';
      }
    }

    if (currentPage === 'pointhistory') {
      if (previousPage === 'home') {
        newTransition = 'slide-right';
      }
    }

    setTransition(newTransition);
    sessionStorage.setItem('path', location.pathname);
  }, [location.pathname]);

  return transition;
};

export default useTransitionSelect;
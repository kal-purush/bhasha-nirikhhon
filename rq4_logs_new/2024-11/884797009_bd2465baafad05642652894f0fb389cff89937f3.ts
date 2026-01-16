import { useContext } from 'react';

import { AppContext } from '@/contexts/app';

const useApp = () => {
  const value = useContext(AppContext);
  if (!value) throw new Error('useApp must be used within a AppProvider');
  return value;
};

export default useApp;
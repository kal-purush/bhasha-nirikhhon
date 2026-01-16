import { useContext } from 'react';
import { DefaultPlaceContext } from '../modules/places/provider';

const useDefaultPlace = () => {
  const context = useContext(DefaultPlaceContext);

  if (context === undefined) {
    throw new Error('please wrap your component with <DefaultPlaceProvider></DefaultPlaceProvider>');
  }

  return context;
};

export default useDefaultPlace;
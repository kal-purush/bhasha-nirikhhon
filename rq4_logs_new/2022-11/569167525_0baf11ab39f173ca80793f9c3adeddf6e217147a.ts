import { useState, useEffect } from 'react';

import { useTheme } from '../theme/useTheme';

import { TSingleTheme } from '../theme/types';
import { THandleSelectedTheme, TUseApp } from './types';

export const useApp: TUseApp = () => {
  const { theme, themeLoaded } = useTheme();
  //useStates
  const [selectedTheme, setSelectedTheme] = useState<TSingleTheme>(theme);

  //handlers
  /**
   * @description This function manage a theme which we want to display. After change theme styles should be changed.
   * @function handleSelectedTheme
   * @category Theme
   */
  const handleSelectedTheme: THandleSelectedTheme = () => setSelectedTheme(theme);

  //useEffects
  useEffect(() => {
    handleSelectedTheme();
  }, [themeLoaded]);

  return {
    selectedTheme,
    themeLoaded
  };
};
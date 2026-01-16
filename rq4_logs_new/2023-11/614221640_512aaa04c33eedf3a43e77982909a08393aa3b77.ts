import { envConfig } from '@core/envconfig';
import { log } from '@core/logger';
import { webRoutes, messageRoutes } from '@core/routes';
import { httpRequest, routeTo } from '@core/utils';
import { useRouting } from '../common';
import { create } from 'zustand';
import { Menu, MenusProps, SlugProps } from '../interface';
import { AllRoutes } from '../utils';
import { enqueueSnackbar } from 'notistack';

export const useSlug = create<SlugProps>((set, get) => ({
  slugs: {
    IDM: '',
    PASM: '',
    'MESSAGE-CATALOG': '',
  },

  getSlug: (key) => {
    const {slugs} = get();
    return slugs?.[key] ?? ''
  },
}));
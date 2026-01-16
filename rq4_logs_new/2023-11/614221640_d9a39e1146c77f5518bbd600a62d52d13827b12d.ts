import { envConfig } from '@core/envconfig';
import { log } from '@core/logger';
import { webRoutes, messageRoutes } from '@core/routes';
import { httpRequest, routeTo } from '@core/utils';
import { useRouting } from '../common';
import { create } from 'zustand';
import { Menu, MenusProps, SlugProps } from '../interface';
import { AllRoutes } from '../utils';
import { enqueueSnackbar } from 'notistack';

export const useAPIKey = create<SlugProps>((set, get) => ({
    APIkey: {
        IDM: '',
        PASM: '',
        ALERTSHUB: '',
        'MESSAGE-CATALOG': '',
    },

    getSlug: (key) => {
        const { APIKey } = get();
        return APIKey?.[key] ?? '';
    },
}));
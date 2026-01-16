import { httpRequest } from '@core/utils';
import { enqueueSnackbar } from 'notistack';
import { create } from 'zustand';
import { ApiDocumentationInterface } from '../interface';
import { envConfig } from '@core/envconfig';
import { useSlug } from '../common';
import { giveMeApiBody } from '../utils';

export const useApiDocumentation = create<ApiDocumentationInterface>((set, get) => ({
    apiBody: giveMeApiBody(),
    apiBodyMessage: '',


    handleChangeCallback: (key: string, value: string) => {
        set((state) => ({ apiBody: { ...state.apiBody, [key]: value } }));
    },

    requestBodyAPI: () => {
        const { apiBody } = get()
        const payload = apiBody
        const slugId = useSlug.getState().slugs.ALERTSHUB;
        httpRequest('post', `${envConfig.api_url}/alertshub/sendmessage`,
            payload,
            true, undefined, {
            headers: {
                slug: slugId,
            },
        })
            .then((response) => {
                // enqueueSnackbar('New Alert Rule successfully Added!', { variant: 'success' });
            })
            .catch((err) => {
                set((state) => ({ apiBodyMessage: err?.response?.data?.message }));

                // enqueueSnackbar(`Oops! Something went wrong, Try Again Later`, { variant: 'error' });
            })
            .finally(() => {
                console.log();
            });
        return false;
    }
}));
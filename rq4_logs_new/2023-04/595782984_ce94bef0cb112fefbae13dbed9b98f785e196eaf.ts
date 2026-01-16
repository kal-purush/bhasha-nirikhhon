import { StateSchema } from 'app/providers/StoreProvider';
import { getArticleDetailsData } from './getArticleDetailsData';

describe('getArticleDetailsData', () => {
    test('should return data', () => {
        const data = {
            id: '1',
            title: 'title',
            subtitle: 'subtitle',
        };

        const state: DeepPartial<StateSchema> = {
            articleDetails: {
                data,
            },
        };
        expect(getArticleDetailsData(state as StateSchema)).toEqual(data);
    });

    test('should work with empty state', () => {
        const state: DeepPartial<StateSchema> = {};
        expect(getArticleDetailsData(state as StateSchema)).toEqual(undefined);
    });
});
/* eslint-disable prettier/prettier */
import { GetAxios } from './series.axios';

export const GetCategories = (
    url: string,
    category: string,
    clear: boolean,
) => {
    return async (dispatch: any,) => {
        let nextLink = '';
        dispatch({ type: 'SET_LOADING', payload: true });
        await GetAxios(url).then(({ data }) => {
            dispatch({ type: 'SET_CATEGORIES', payload: data?.data, category, clear });
            nextLink = data?.links?.next;
        }).catch((err) => console.log(err.message));
        dispatch({ type: 'SET_LOADING', payload: false });
        return nextLink;
    };
};

export const GetSeries = (
    url: string,
    category: string,
    categoryId: number,
) => {
    return async (dispatch: any,) => {
        let nextLink = '';
        let series: any = [];
        dispatch({ type: 'SET_LOADING', payload: true });
        await GetAxios(url).then(({ data }) => {
            dispatch({ type: 'SAVE_SERIES', payload: { categoryId, series: data?.data }, category });
            series = data?.data;
            nextLink = data?.links?.next;
        }).catch((err) => { throw err.message; });
        dispatch({ type: 'SET_LOADING', payload: false });
        return { series, nextLink };
    };
};

export const GetOfflineSeries = (category: string,
    categoryId: number,) => {
    return async (dispatch: any, getState: any) => {
        let series: any = [];
        const { Series } = getState();
        dispatch({ type: 'SET_LOADING', payload: true });
        if (category === 'anime') {
            series = Series.animeSeries.find((serie: any) => serie?.categoryId === categoryId).series;
        } else {
            series = Series.mangaSeries.find((serie: any) => serie?.categoryId === categoryId).series;
        }
        dispatch({ type: 'SET_LOADING', payload: false });
        return series;
    };
};

export const GetSearch = (
    url: string
) => {
    return async (dispatch: any) => {
        let nextLink = '';
        let series: any = [];
        dispatch({ type: 'SET_LOADING', payload: true });
        await GetAxios(url).then(({ data }) => {
            series = data?.data;
            nextLink = data?.links?.next;
        }).catch((err) => { throw err.message; });
        dispatch({ type: 'SET_LOADING', payload: false });
        return { series, nextLink };
    };
};

export const GetOfflineSearch = (category: string, text: string) => {
    return async (dispatch: any, getState: any) => {
        const { Series } = getState();
        dispatch({ type: 'SET_LOADING', payload: true });
        const getTitle = (item: any) =>
            item?.attributes?.titles['en']
                ? item?.attributes?.titles['en']
                : item?.attributes?.titles['en_jp']
                    ? item?.attributes?.titles['en_jp']
                    : item?.attributes?.titles['en_kr']
                        ? item?.attributes?.titles['en_kr']
                        : 'Sin título';

        let allSeries = [];
        if (category === 'anime') {
            allSeries = Series.animeSeries.map((serie: any) => serie.series).flat().filter((serie: any) => getTitle(serie).toLowerCase().includes(text));
            console.log(allSeries);
        } else {
            allSeries = Series.mangaSeries.map((serie: any) => serie.series).flat().filter((serie: any) => getTitle(serie).toLowerCase().includes(text));
        }
        dispatch({ type: 'SET_LOADING', payload: false });
        return allSeries;
    };
};
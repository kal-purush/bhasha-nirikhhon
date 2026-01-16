/* eslint-disable prettier/prettier */
import produce from 'immer';

const initState = {
    favoriteSeries: [],
};

const Series = (state = initState, action: any) =>
    produce(state, (draft) => {
        switch (action.type) {
            case 'SET_FAVORITE':
                console.log(action);
                let currentSeries = [...state.favoriteSeries];
                if (currentSeries.some(item => item?.id === action.payload?.id)) {
                    draft.favoriteSeries = currentSeries.filter(item => item?.id !== action.payload?.id);
                } else {
                    currentSeries.push(action.payload);
                    draft.favoriteSeries = currentSeries;
                }
                break;
            default:
                return state;
        }
    });

export default Series;
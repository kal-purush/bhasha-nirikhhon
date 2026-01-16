/**
 * Created by youngmoon on 2/12/16.
 */

export const onToggle = (index) => {
    return {
        type: 'TOGGLE_TODO',
        index
    };
};

export const setVisible = (filter) => {
    return {
        type: 'SET_VISIBLE',
        filter
    };
};

export const onSubmit = (text) => {
    return {
        type: 'ADD_TODO',
        text
    };
};

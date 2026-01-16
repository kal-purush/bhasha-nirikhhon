export const todo = (state, action) => {
    switch (action.type) {
        case 'ADD_TODO':
            return {
                id: action.id,
                text: action.text,
                completed: false
            };
        case 'TOGGLE_TODO':
            if (state.id !== action.id) {
                return state;
            }

            return Object.assign({}, state, {
                completed: !state.completed
            });
        default:
            return state;
    }
};

export const todos = (state = [], action) => {
    switch (action.type) {
        case 'ADD_TODO':
            return [
                ...state,
                todo(undefined, action)
            ];
        case 'TOGGLE_TODO':
            return state.map(t =>
                todo(t, action)
            );
        case 'REMOVE_TODO':
            return state.filter(t =>
                t.id !==action.id
            );
        default:
            return state;
    }
};

export const currentFilter = (state = 'SHOW_ALL', action) => {
    switch (action.type) {
        case 'SET_CURRENT_FILTER':
            console.log(state, action.filter)
            return action.filter;
        default:
            return state;
    }
};
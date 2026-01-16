const initialState = {
    email: '',
    token: '',
    isLogged: false,
};

export const user = (state = initialState, action: any) => {
    switch (action.type) {
        case 'LOG_IN':
            return {
                ...state,
                email: action.payload.email,
                token: action.payload.token,
                isLogged: true
            };

        case 'LOG_OUT':
            return {
                ...initialState
            };

        default:
            return state
    }
};
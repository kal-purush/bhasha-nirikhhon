export const STATUS_ERROR = 'error';
export const STATUS_SUCCESS = 'success';

const reducer = (state, action) => {
    switch (action.type) {
        case 'loginSuccess':
            return {
                ...state,
                auth: action.payload.data.token,
                name: action.payload.data.user,
                status: {
                    type: STATUS_SUCCESS,
                    message: action.payload.data.message
                },
                isAuth: true
            };
        case 'loginError':
            return {
                ...state,
                status: {
                    type: STATUS_ERROR,
                    message: action.payload.error
                }
            };
        case 'logoutUser':
            return {
                ...state,
                auth: '',
                name: '',
                isAuth: false
            };
        case 'registrationSuccess':
            return {
                ...state,
                status: {
                    type: STATUS_SUCCESS,
                    message: action.payload.data.message
                }
            };
        case 'registrationFailed':
            return {
                ...state,
                status: {
                    type: STATUS_ERROR,
                    message: action.payload.error
                }
            };
        default:
            return state;
    }
};

export default reducer;
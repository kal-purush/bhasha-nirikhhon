import produce from 'immer';
// 액션 타입
const SET_AWS_KEY = 'settings/aws/setAWSKey';

interface SetAWSKey {
    type: typeof SET_AWS_KEY;
    payload: {
        accessKey: string;
        secretKey: string;
    };
}

type ActionTypes = SetAWSKey;
// 액션 생성자
export const setAWSKey = (accessKey: string, secretKey: string) => {
    return {
        type: SET_AWS_KEY,
        payload: { accessKey, secretKey },
    };
};

// 리듀서
const initState = {
    accessKey: localStorage.getItem('accesskey') || '',
    secretKey: localStorage.getItem('secretkey') || '',
};
const reducer = (state = initState, action: ActionTypes) => {
    return produce(state, draft => {
        switch (action.type) {
            case SET_AWS_KEY:
                draft.accessKey = action.payload.accessKey;
                draft.secretKey = action.payload.secretKey;
            default:
                break;
        }
    });
};

export default reducer;
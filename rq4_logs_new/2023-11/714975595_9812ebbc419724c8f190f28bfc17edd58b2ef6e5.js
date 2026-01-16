export const CHANGE = "CHANGE";

export const changeDisplay = (audioType) => {
    return {
        type: CHANGE, display: audioType
    }
}

export const displayReducer = (state = {display: ""}, action) => {
    switch (action.type) {
        case CHANGE:
            return {
                display: action.display
            }
            break;
        default:
            return state;
    }
};

export const mapStateToProps = (state) => {
    return {
        display: state.display
    }
}

export const mapDispatchToProps = (dispatch) => {
    return {
        changeDisplay: () => {dispatch(changeDisplay)}
    }
}
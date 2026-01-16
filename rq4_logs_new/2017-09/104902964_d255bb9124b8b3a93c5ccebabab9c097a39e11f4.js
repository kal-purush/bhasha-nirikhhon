export const getUsersReceive = (users) => {
    return {
        type: 'GET_USERS_RECEIVE',
        users
    };
};

export const getUsers = (page) => (dispatch) => {
    fetch('https://api.github.com/users?per_page=100&since=' + page * 100)
        .then((response) => response.json())
        .then((users) => dispatch(getUsersReceive(users)));
};

export const getUserReceive = (user) => {
    return {
        type: 'GET_USER_RECEIVE',
        user
    };
};

export const getUser = (user) => (dispatch) => {
    fetch('https://api.github.com/users/' + user)
        .then((response) => response.json())
        .then((user) => dispatch(getUserReceive(user)));
};
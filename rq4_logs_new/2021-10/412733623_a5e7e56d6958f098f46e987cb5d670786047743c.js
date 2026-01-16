const follow = "FOLLOW";
const unfollow = "UNFOLLOW";
const setUsers = "SET_USERS";

let initialState = {
    users: [],
};

let findUsersReducer = (state = initialState, action) => {
    switch (action.type) {
        case follow: {
            return {
                ...state,
                users: state.users.map((user) => (user.id === action.userId ? { ...user, following: true } : user)),
            };
        }
        case unfollow: {
            return {
                ...state,
                users: state.users.map((user) => (user.id === action.userId ? { ...user, following: false } : user)),
            };
        }
        case setUsers: {
            return { ...state, users: [...state.users, ...action.users] };
        }
        default:
            return state;
    }
};

export let followAC = (userId) => ({ type: follow, userId: userId });
export let unfollowAC = (userId) => ({ type: unfollow, userId: userId });
export let setUsersAC = (users) => ({ type: setUsers, users });

export default findUsersReducer;
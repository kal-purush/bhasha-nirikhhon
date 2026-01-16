// here we are going to work only with redux and not react
// so this is a js file which we will run with nodejs

// STEPS to run this file
// 1. install redux libary
//          npm install redux
// 2. fininsh the code in this file
// 3. run this file
//          navigate to the folder of this file and runn  the command
//                  node redux-demo-1.js

// code
// 1. create the action object - not required, because only step 2 is needed
const redux = require("redux");

const createStore = redux.createStore;

const BUY_A_COOKIE = "BUY_A_COOKIE";

{
  type: BUY_A_COOKIE;
  payload: "";
}

// 2 . create an action creator function
//        - this function will return the action object
function buyCookieActionCreator() {
  return {
    type: BUY_A_COOKIE,
    payload: ""
  };
}

// 3. create the reducer function
const initialState = {
  numOfCookies: 10
};

const reducer = (state = initialState, action) => {
  switch (action.type) {
    case BUY_A_COOKIE:
      return {
        ...state,
        numOfCookies: state.numOfCookies - 1
      };
    default:
      return state;
  }
};

// 4. Implement the redux Store
const store = createStore(reducer);

console.log("Initial state of the store:", store.getState());

const unsubscribe = store.subscribe(() =>
  console.log("State:", store.getState())
);

store.dispatch(buyCookieActionCreator());
store.dispatch(buyCookieActionCreator());
store.dispatch(buyCookieActionCreator());
store.dispatch(buyCookieActionCreator());
store.dispatch(buyCookieActionCreator());
store.dispatch(buyCookieActionCreator());

unsubscribe();
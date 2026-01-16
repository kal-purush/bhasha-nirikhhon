import * as redux from 'redux';

interface User {
  first: string;
  last: string;
}

interface MyAppState {
  appLoading: boolean;
  users?: User[]
}

//marker interface for actions
type MyAppAction = FetchUsersAction | ReceiveUsersAction;


//Action Type which denotes an ajax call being kicked off
class FetchUsersAction {
  thing: boolean;
}

//Action Type which denotes the user ajax call being returned
class ReceiveUsersAction {
  users: User[]
}

const rootReducer = (state: MyAppState, action: MyAppAction): MyAppState => {
  if(action instanceof FetchUsersAction){
    return Object.assign({}, state, {
      loading: true,
      hmm:true
    });
  } else if(action instanceof ReceiveUsersAction){
    Object.assign({}, state, {
      loading: true,
      users: action.users
    });
  }
  return state;
}

const initialState: MyAppState = {
  appLoading: false
};

const store = redux.createStore(rootReducer, initialState);

console.log(store.getState());
store.dispatch(new FetchUsersAction());
console.log(store.getState());



interface Person {
  name: string
}

//fails
const nobody: Person = {};

//fails
const paul: Person = {
  name: "Paul"
};

//error, unknown attr
const nicole: Person = {
  name: "Nicole",
  anotherAttr: "test"
};

//error, missing attr
const nobody2: Person = Object.assign({}, {});

//succeeds
const paul2: Person = Object.assign({}, {
  name: "Paul"
});

//error, missing attr
const nicole2: Person = Object.assign({}, {
  name: "Nicole",
  anotherAttr: "test"
});





//redux.dispatch(new FetchUsersAction())
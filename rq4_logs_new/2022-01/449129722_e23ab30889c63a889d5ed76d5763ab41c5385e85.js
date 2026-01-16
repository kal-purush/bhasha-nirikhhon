const timeoutIdReducer = (state='', action)=>{
  switch(action.type){
    case 'SET_TIMEOUTID':
      return action.timeoutId
    default:
      return state
  }
}

export default timeoutIdReducer
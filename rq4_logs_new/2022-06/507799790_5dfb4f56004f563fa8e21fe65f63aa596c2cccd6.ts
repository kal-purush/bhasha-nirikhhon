
import { Action } from "../actions";
const initialState={
    balance:500
}

const reducer=(state=initialState,action:Action)=>{
switch(action.type){
    case "deposit":
        return {...state,
            balance:state.balance + Number(action.payload)
        }
        
        
    case "withdraw":
        return {
            balance:state.balance-action.payload
        }

    default:
        return state;
}
}
export default reducer;
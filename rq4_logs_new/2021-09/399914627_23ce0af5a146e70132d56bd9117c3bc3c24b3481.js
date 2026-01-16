const initialState = {
	userAtMount:"Freddy"
}

const HooksExampleSlice = (state = initialState, action) =>{
	switch(action.type){
		case "updateUserAtMount":
			return ({...state, userAtMount:action.payload})
			break;
		default:
			return state;
	}
}

export default HooksExampleSlice;
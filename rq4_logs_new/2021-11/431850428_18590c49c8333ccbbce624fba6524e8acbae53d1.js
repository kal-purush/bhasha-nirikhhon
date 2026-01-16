import { initialState } from '../store'
import {ADD_TO_FAVOIRITES, REMOVE_FROM_FAVOURITES} from "../actions"



const favouritesReducer = (state = initialState, action) => {
    
    switch (action.type) {
     
            case ADD_TO_FAVOIRITES:
                return {...state,
                    favourites: [...state.favourites, action.payload]}
            case REMOVE_FROM_FAVOURITES:
                return {...state,
                        favourites: state.favourites.filter((el) => el !== action.payload)}        
            default: 
                return state

    }
}



export default favouritesReducer
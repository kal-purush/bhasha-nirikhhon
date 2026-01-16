import alt from '../../alt';
import axios from 'axios';

class RegistrationActions {
    register(args){

      return (dispatch) => {
        axios
          .post('/api/grocerystore', args)
          .then(res => dispatch(res))
          .catch(res => dispatch(res))
      }
    }
}

export const actions = alt.createActions(RegistrationActions);

class RegistrationStore {
    constructor(){
      this.bindActions(actions);
      this.state = {
        showRegisterStoreForm : true,
        backendValidationErrors : {}
      }
    }
    register(registrationResponse) {
      if (registrationResponse.status === 200) {
        this.setState({showRegisterStoreForm : false });
      } else if (registrationResponse.status === 400){
        this.setState({backendValidationErrors : registrationResponse.data});
      }
    }
}

export const store = alt.createStore(RegistrationStore);
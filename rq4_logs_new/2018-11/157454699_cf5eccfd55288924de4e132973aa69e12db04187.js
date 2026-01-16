import uuidv4 from 'uuid/v4';
import axios from 'axios';
import store from '../../redux/store';
import { updateUserInfo } from '../../redux/actions';

export const createNewUser = user => {  
  user['id'] = uuidv4();
  user['firstName'] = '';
  user['lastName'] = '';
  return new Promise((resolve, reject) => {
    axios.post('/users', user).then(resp => {    
      if(resp.data.statusCode === 200) {
        store.dispatch(updateUserInfo(user));
        resolve({ registration: true, data: resp.data });
      } else {      
        reject({ registration: false, data: resp.data });
      }
    });
  })
}
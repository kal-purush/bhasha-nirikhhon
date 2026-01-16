import * as constants from "./constants"
import axios from "axios";

const changeLogin = () => {
  return {
    type: constants.LOGIN,
    value: true
  }
};

export const login = (accout, password) => {

  return (dispatch) => {
    axios.get('/api/login.json?account=' + accout + '&password=' + password).then((res) => {
      const result = res.data.data;
      if (result) {
        console.log(result);
        dispatch(changeLogin())
      }else {
        alert('登陆失败')
      }
    });
  }
};
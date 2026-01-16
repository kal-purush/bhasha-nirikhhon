import Axios from 'axios';

export default class AjaxWorker {
  url = 'https://localhost:5000/'

  async request(url) {
    let response = 'NOINTERNETCONNECTION';
    await Axios.get(this.url + url).then((connectionResponse) => {
      response = connectionResponse;
    }).catch((e) => {
      console.log('connection error');
      console.log(e);
    });
    return response;
  }
}
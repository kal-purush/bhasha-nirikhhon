import request from 'request';

const receiveOkUrl = (url) => ({
  type: 'RECEIVE_OK_URL',
  url: url
});


export const requestProxySearch = () => (dispatch) => {
  startSearch(dispatch);
  console.log('done');
}


const startSearch = (dispatch) => {
  let baseUrl = 'http://172.16.';
  console.log('started requesting');
  let count = 0;
  for (let i = 110; i < 256; i++) {
    for (let j = 0; j < 256; j++) {
      count++;
      let proxyUrl = baseUrl + i.toString() + '.' + j.toString() + ':3128';
      console.log('requesting ' + proxyUrl);
      var proxiedRequest = request.defaults({ 'proxy': proxyUrl });
      proxiedRequest.get("http://google.com", function (err, resp, body) {
        if(resp !== undefined) console.log(resp, proxyUrl);
        if (resp !== undefined && resp.statusCode === 200) {
          console.log(proxyUrl);
          dispatch(receiveOkUrl(proxyUrl));
        }
      });
    }
  }
  console.log('Finished requesting ' + count + ' proxies. Waiting for response.');
}
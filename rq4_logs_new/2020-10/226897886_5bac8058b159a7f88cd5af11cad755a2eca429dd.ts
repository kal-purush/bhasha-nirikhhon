// http.test.ts
import axios, { AxiosRequestConfig } from 'axios';
import HttpService from './http';
import Configuration from '../configuration/Configuration';


jest.mock('axios');

test('getUserById - should fetch a user by id', async () => {
  const user = {"id":1, "deliveryDays":"1"};
  const resp = {data: user};
   
  axios.mockResolvedValueOnce(resp);
  
  // or you could use the following depending on your use case:
  const actualUser = await HttpService.getUserById(1);
  expect(actualUser).toEqual(user);
  expect(axios).toBeCalledTimes(1);
  expect(axios).toBeCalledWith(expect.objectContaining({
    method: 'get',
    url: '/users/1',
  }));   
});
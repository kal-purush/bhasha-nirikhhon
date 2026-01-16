import { HomeController } from '../../../../main/controllers';
import { mockRequest, mockResponse } from '../../mocks';

/* eslint-disable jest/expect-expect */
describe('Home Controller', () => {
  const controller = new HomeController();
  test('Should render home page', async () => {
    // eslint-disable-line @typescript-eslint/no-empty-function
    const req = mockRequest(null);
    const res = mockResponse();
    controller.get(req, res);
    expect(res.render).toBeCalledWith('home');
  });
});
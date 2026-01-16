import { Response, Request } from 'express';
import Github from '../../controllers/github';

const mockResponse = {
  redirect: jest.fn(),
};

const mockRequest = {
  query: {
    code: '123',
  },
};

describe('Test the Github Controller', () => {
  it('Testing the mainAction', () => {
    // @ts-ignore
    const responseMock: Response = { ...mockResponse };
    // @ts-ignore
    const requestMock: Request = { ...mockRequest };

    Github.mainAction(requestMock, responseMock);

    expect(responseMock.redirect)
      .toBeCalledTimes(1);
    expect(responseMock.redirect)
      .toHaveBeenCalledWith('/user/123/');
  });
});
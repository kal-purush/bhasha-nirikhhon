import { UseCase, Controller } from '../../types';

const authenticate = ({
  authenticateUseCase,
}: {
  authenticateUseCase: UseCase<string>;
}): Controller => {
  return async function controller(httpRequest) {
    try {
      const { source = {}, ...info } = httpRequest.body;
      source.ip = httpRequest.ip;
      source.browser = httpRequest.headers['User-Agent'];
      if (httpRequest.headers.Referer) {
        source.referrer = httpRequest.headers.Referer;
      }
      const token = await authenticateUseCase({
        id: null,
        info,
        source,
      });
      return {
        headers: {
          'Content-Type': 'application/json',
        },
        statusCode: 200,
        body: { token },
      };
    } catch (e) {
      return {
        headers: {
          'Content-Type': 'application/json',
        },
        statusCode: 400,
        body: {
          error: e.message,
        },
      };
    }
  };
};

export default authenticate;
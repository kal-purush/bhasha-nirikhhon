import axios from 'axios';

const apiMiddleware = config => next => (action) => {
  if (!config) throw Error('A config object is required in order to use redux-basic-http-middleware.');
  if (!config.baseUrl) throw Error('A config baseUrl is required in order to use redux-basic-http-middleware.');

  const baseUrl = config.baseUrl;
  const {
    type,
    promise: { url, options },
    ...rest
  } = action;

  if (!url) return next(action);

  const SUCCESS = `${type}_SUCCESS`;
  const REQUEST = `${type}_REQUEST`;
  const FAILURE = `${type}_FAILURE`;

  next({ ...rest,
    type: REQUEST
  });
  return axios(baseUrl + url, options)
    .then((res) => {
      next({ ...rest,
        data: res.data,
        type: SUCCESS
      });
    })
    .catch((err) => {
      next({ ...rest,
        err,
        type: FAILURE
      });
    });
};

export default apiMiddleware;
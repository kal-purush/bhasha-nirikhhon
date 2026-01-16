import ajax from 'services/ajax';

export const getApiKeys = ({ userId }) => {
  return ajax.get(`/o/api_key?user_id=${userId}`).then(r => ({ resultSet: r.data }));
};
import axios from 'axios';
import API_URL from 'utils/ApiUrl';
import { authHeader } from './auth/AuthHeaders';

const getFindAllServiceTypes = () => {
  return axios.get(`${API_URL}/servicetype`, {
    headers: authHeader(),
  });
};

const ServiceTypeService = {
  getFindAllServiceTypes,
};

export default ServiceTypeService;
import axios from 'axios';
import { handleApiError } from './handle-api-error';

const NEW_FORM_URL = `${process.env.NEXT_PUBLIC_BACKEND_BASE_URL}/new-form`;

export const FetchAllForm = async () => {
  try {
    const response = await axios.get(NEW_FORM_URL, {
      withCredentials: true,
      params: ""
    });

    return response.data;
  } catch (error) {
    return handleApiError(error, "Failed to fetch forms");
  }
};

export const isNotCompletedForm = async () => {
  try {
    const response = await axios.get(NEW_FORM_URL, {
      withCredentials: true,
      params: { completed: false },
    });

    return response.data;
  } catch (error) {
    return handleApiError(error, "Failed to fetch incomplete forms");
  }
};

export const createNewForm = async (name: string) => {
  try {
    const response = await axios.post(NEW_FORM_URL, { name }, {
      withCredentials: true,
    });

    return response.data;
  } catch (error) {
    return handleApiError(error, "Failed to create new form");
  }
};
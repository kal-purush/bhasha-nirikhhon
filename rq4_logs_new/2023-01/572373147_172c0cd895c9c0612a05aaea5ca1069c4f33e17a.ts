import { axiosInstance } from '../../apiInteractor/apiInstance';
import { informativeErrorBuilder } from '../../errors/builders/InformativeErrorBuilder';
import { InformativeError } from '../../errors/InformativeError';
import { UrlContainer } from '../UrlContainer';
import { addUrlContainerConfig } from './addUrlContainerConfig';

export const addUrlContainerInteractor = async (
  urlContainer: UrlContainer
): Promise<UrlContainer | InformativeError> => {
  try {
    const response = await axiosInstance.post(
      '/containers',
      addUrlContainerConfig
    );

    const urlContainer: UrlContainer = response.data as UrlContainer;

    return urlContainer;
  } catch (error: any) {
    return informativeErrorBuilder('Failed to add container', error.message);
  }
};
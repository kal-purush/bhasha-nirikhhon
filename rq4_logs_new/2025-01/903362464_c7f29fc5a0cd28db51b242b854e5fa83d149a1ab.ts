import { axiosInstance } from '@/apis/axios-instance';
import { MyActivitiesCreateData } from '@/types/my-activities-create-data';
import { useMutation } from '@tanstack/react-query';

export const useMyActivitiesCreate = () =>
  useMutation({
    mutationFn: (data: MyActivitiesCreateData) => {
      const token = localStorage.getItem('accessToken');
      if (!token) {
        throw new Error('No access token found');
      }

      return axiosInstance.post('/activities', data, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
    },
  });
import { axiosInstance } from '@/apis/axios-instance';

// res1
export const fetchMyActivityList = async () => {
  try {
    const url = 'my-activities?size=1000';
    // console.log('url: ', url);

    const response = await axiosInstance.get(url, {});

    return response.data;
  } catch (error) {
    console.error('fetchMyActivityList에서 에러 발생:', error);
    throw new Error('fetchMyActivityList에서 에러 발생');
  }
};

// res2
export const fetchMyCalendarEvent = async (activityId: number, year: string, month: string) => {
  try {
    // year month 각각 4/2자리 string, 0 채워서
    const url = `my-activities/${activityId}/reservation-dashboard?year=${year}&month=${month}`;
    // console.log('url: ', url);

    const response = await axiosInstance.get(url, {});

    return response.data;
  } catch (error) {
    console.error('fetchMyCalendarEvent에서 에러 발생:', error);
    throw new Error('fetchMyCalendarEvent에서 에러 발생');
  }
};

// export const fetchMyActivityList = async (pageParam: null | number, queryKey: string[], isFiltered: boolean) => {
//   try {
//     const cursorUrl = isFiltered ? `/${queryKey}&cursorId=${pageParam}` : `/${queryKey}?cursorId=${pageParam}`;
//     const url = pageParam === null ? `/${queryKey}` : cursorUrl;

//     // console.log('url: ', url);

//     const response = await axiosInstance.get(url, {});

//     return response.data;
//   } catch (error) {
//     console.error('fetchMyReservations에서 에러 발생:', error);
//     throw new Error('fetchMyReservations에서 에러 발생');
//   }
// };

// export const cancelMyReservations = async (reservationId: number) => {
//   try {
//     const url = `my-reservations/${reservationId}`;
//     // console.log('url: ', url);

//     const response = await axiosInstance.patch(url, {
//       status: 'canceled',
//     });

//     return response.data;
//   } catch (error) {
//     console.error('cancelMyReservations에서 에러 발생:', error);
//     throw new Error('cancelMyReservations에서 에러 발생');
//   }
// };

// export const reviewMyReservations = async (reservationId: number, rating: number, content: string) => {
//   try {
//     const url = `my-reservations/${reservationId}/reviews`;
//     // console.log('url: ', url);

//     const review = {
//       rating: rating,
//       content: content,
//     };

//     const response = await axiosInstance.post(url, review);

//     return response.data;
//   } catch (error) {
//     console.error('reviewMyReservations에서 에러 발생:', error);
//     throw new Error('reviewMyReservations에서 에러 발생');
//   }
// };
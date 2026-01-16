// import { fetchMyActivityList } from '@/apis/my-activity-board';
// import { useInfiniteQuery } from '@tanstack/react-query';

// export const useMyActivityList = () => {
//   return useInfiniteQuery({
//     queryKey: ['my-activities'],
//     queryFn: ({ pageParam = null }) => fetchMyActivityList(pageParam),
//     initialPageParam: null,
//     getNextPageParam: (lastPage) => {
//       return lastPage.cursorId; // 더 없으면 null
//     },
//     staleTime: 1000 * 60,
//   });
// };

// import { fetchMyReservations } from '@/apis/my-reservations';
// import { STATUS_TRANSLATE } from '@/constants/RESERVATION_STATUS';
// import { useInfiniteQuery } from '@tanstack/react-query';

// export const useMyReservations = (status: string | null) => {
//   let engStatus = '';
//   let isFiltered = false;

//   if (
//     status === '예약 신청' ||
//     status === '예약 승인' ||
//     status === '예약 거절' ||
//     status === '예약 취소' ||
//     status === '체험 완료'
//   ) {
//     isFiltered = true;
//     engStatus = STATUS_TRANSLATE[status];
//   }

//   return useInfiniteQuery({
//     queryKey: isFiltered ? [`my-reservations?status=${engStatus}`] : ['my-reservations'],
//     queryFn: ({ pageParam = null, queryKey }) => fetchMyReservations(pageParam, queryKey, isFiltered),
//     initialPageParam: null,
//     getNextPageParam: (lastPage) => {
//       return lastPage.cursorId; // 더 없으면 null
//     },
//     staleTime: 1000 * 60,
//   });
// };
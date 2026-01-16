import { ResForScheduleData } from '@/components/profile/reservations/status/MyActivityModal';

export const response1 = {
  activities: [
    {
      id: 3568,
      userId: 1484,
      title: '클라이밍 원데이 클래스',
      description: '클라이밍 대회 수상자가 지도하는 최대 6인 원데이 클래스',
      category: '스포츠',
      price: 35000,
      address: '서울특별시 강남구 테헤란로10길 21',
      bannerImageUrl:
        'https://sprint-fe-project.s3.ap-northeast-2.amazonaws.com/globalnomad/activity_registration_image/10-2_1484_1735948416435.png',
      rating: 0,
      reviewCount: 0,
      createdAt: '2025-01-04T08:55:08.798Z',
      updatedAt: '2025-01-04T08:55:08.798Z',
    },
    {
      id: 3567,
      userId: 1484,
      title: '성수동 가죽공방 체험 - 미니 가방 만들기',
      description: '염색부터 이니셜 각인까지 장인 거리 성수동 공방에서 체험해보세요',
      category: '문화 · 예술',
      price: 60000,
      address: '서울특별시 성동구 연무장7길 13 지하1층',
      bannerImageUrl:
        'https://sprint-fe-project.s3.ap-northeast-2.amazonaws.com/globalnomad/activity_registration_image/10-2_1484_1735948078850.jpeg',
      rating: 0,
      reviewCount: 0,
      createdAt: '2025-01-04T08:48:34.436Z',
      updatedAt: '2025-01-04T08:48:34.436Z',
    },
    {
      id: 3566,
      userId: 1484,
      title: '12년 경력 뷰티업계 현직자와 함께하는 퍼스널 컬러 찾기',
      description: '퍼컬 찾고 딱 맞는 코스메틱 추천까지',
      category: '문화 · 예술',
      price: 30000,
      address: '서울특별시 성동구 왕십리로 92',
      bannerImageUrl:
        'https://sprint-fe-project.s3.ap-northeast-2.amazonaws.com/globalnomad/activity_registration_image/10-2_1484_1735947539816.jpeg',
      rating: 5,
      reviewCount: 1,
      createdAt: '2025-01-04T08:40:23.090Z',
      updatedAt: '2025-01-11T13:16:49.847Z',
    },
  ],
  totalCount: 3,
  cursorId: null,
};

// id: 3566, 퍼컬 체험
// 2025-01
export const response2 = [
  {
    date: '2025-01-10',
    reservations: {
      completed: 1,
      confirmed: 0,
      pending: 0,
    },
  },
  {
    date: '2025-01-12',
    reservations: {
      completed: 2,
      confirmed: 0,
      pending: 0,
    },
  },
  {
    date: '2025-01-18',
    reservations: {
      completed: 0,
      confirmed: 2,
      pending: 0,
    },
  },
  {
    date: '2025-01-25',
    reservations: {
      completed: 0,
      confirmed: 2,
      pending: 3,
    },
  },
];

// id: 3566, date: 2025-01-25(string)

export const response3 = [
  {
    scheduleId: 13407,
    startTime: '12:00',
    endTime: '14:00',
    count: {
      declined: 0,
      confirmed: 0,
      pending: 1,
    },
  },
  {
    scheduleId: 13408,
    startTime: '14:00',
    endTime: '16:00',
    count: {
      declined: 0,
      confirmed: 0,
      pending: 1,
    },
  },
];

// id: 3566, scheduledId: 13407, status: pending

export const response4: { reservations: ResForScheduleData[]; totalCount: number; cursorId: number | null } = {
  reservations: [
    {
      id: 5516,
      status: 'pending',
      totalPrice: 90000,
      headCount: 3,
      nickname: 'Jay',
      userId: 1484,
      date: '2025-01-25',
      startTime: '12:00',
      endTime: '14:00',
      createdAt: '2025-01-04T09:06:58.778Z',
      updatedAt: '2025-01-04T09:06:58.778Z',
      activityId: 3566,
      scheduleId: 13407,
      reviewSubmitted: false,
      teamId: '10-2',
    },
    {
      id: 5517,
      status: 'pending',
      totalPrice: 90000,
      headCount: 3,
      nickname: 'Jay',
      userId: 1484,
      date: '2025-01-25',
      startTime: '12:00',
      endTime: '14:00',
      createdAt: '2025-01-04T09:06:58.778Z',
      updatedAt: '2025-01-04T09:06:58.778Z',
      activityId: 3566,
      scheduleId: 13407,
      reviewSubmitted: false,
      teamId: '10-2',
    },
    {
      id: 5518,
      status: 'confirmed',
      totalPrice: 90000,
      headCount: 4,
      nickname: 'Jimin',
      userId: 1484,
      date: '2025-01-25',
      startTime: '12:00',
      endTime: '14:00',
      createdAt: '2025-01-04T09:06:58.778Z',
      updatedAt: '2025-01-04T09:06:58.778Z',
      activityId: 3566,
      scheduleId: 13407,
      reviewSubmitted: false,
      teamId: '10-2',
    },
    {
      id: 5519,
      status: 'declined',
      totalPrice: 90000,
      headCount: 4,
      nickname: 'Juice',
      userId: 1484,
      date: '2025-01-25',
      startTime: '12:00',
      endTime: '14:00',
      createdAt: '2025-01-04T09:06:58.778Z',
      updatedAt: '2025-01-04T09:06:58.778Z',
      activityId: 3566,
      scheduleId: 13407,
      reviewSubmitted: false,
      teamId: '10-2',
    },
  ],
  totalCount: 1,
  cursorId: null,
};

// id: 3566, scheduledId: 13407, status: declined, confirmed

export const response5 = {
  reservations: [],
  totalCount: 0,
  cursorId: null,
};
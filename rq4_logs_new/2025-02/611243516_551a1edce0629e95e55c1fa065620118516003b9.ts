//
//    September 2022
// Su Mo Tu We Th Fr Sa
//              1  2  3
//  4  5  6  7  8  9 10
// 11 12 13 14 15 16 17
// 18 19 20 21 22 23 24
// 25 26 27 28 29 30
//
type MockEvent = {
  id: number;
  title: string;
  times?: {
    startDateTime: Date;
    endDateTime?: Date;
  }[];
};
export const mockEvents: MockEvent[] = [
  {
    id: 1,
    title: 'One hour event',
    times: [
      {
        startDateTime: new Date('2022-09-27T13:00:00.000Z'),
        endDateTime: new Date('2022-09-27T14:00:00.000Z'),
      },
    ],
  },
  {
    id: 2,
    title: 'All day event',
    times: [
      {
        startDateTime: new Date('2022-09-27T08:00:00.000Z'),
        endDateTime: new Date('2022-09-27T18:00:00.000Z'),
      },
    ],
  },
  {
    id: 3,
    title: 'Two day event; Thursday-Friday',
    times: [
      {
        startDateTime: new Date('2022-09-22T08:00:00.000Z'),
        endDateTime: new Date('2022-09-23T18:00:00.000Z'),
      },
    ],
  },
  {
    id: 4,
    title: 'May to September exhibition',
    times: [
      {
        startDateTime: new Date('2022-05-01T08:00:00.000Z'),
        endDateTime: new Date('2022-09-30T18:00:00.000Z'),
      },
    ],
  },
  {
    id: 5,
    title: 'Multi day event',
    times: [
      {
        startDateTime: new Date('2022-09-22T12:00:00.000Z'),
        endDateTime: new Date('2022-09-22T14:00:00.000Z'),
      },
      {
        startDateTime: new Date('2022-10-22T12:00:00.000Z'),
        endDateTime: new Date('2022-10-23T14:00:00.000Z'),
      },
      {
        startDateTime: new Date('2022-11-22T12:00:00.000Z'),
        endDateTime: new Date('2022-11-23T14:00:00.000Z'),
      },
    ],
  },
  {
    id: 6,
    title: 'May exhibition with no end time (permanent)',
    times: [
      {
        startDateTime: new Date('2022-05-22T08:00:00.000Z'),
      },
    ],
  },
  {
    id: 7,
    title: 'Today: Not passed yet',
    times: [
      {
        startDateTime: new Date('2022-09-05T16:00:00.000Z'),
        endDateTime: new Date('2022-09-05T17:00:00.000Z'),
      },
    ],
  },
  {
    id: 8,
    title: 'Today: Currently happening',
    times: [
      {
        startDateTime: new Date('2022-09-05T13:00:00.000Z'),
        endDateTime: new Date('2022-09-05T15:00:00.000Z'),
      },
    ],
  },
  {
    id: 9,
    title: 'Today: in the past',
    times: [
      {
        startDateTime: new Date('2022-09-05T08:00:00.000Z'),
        endDateTime: new Date('2022-09-05T10:00:00.000Z'),
      },
    ],
  },
];
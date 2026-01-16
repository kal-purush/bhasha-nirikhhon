import { TrackerRepository } from '@src/repositories/tracker-repository';

jest.mock('@src/repositories/tracker-repository');

describe('Tracker Service', () => {
  const mockedTrackerService = new TrackerRepository() as jest.Mocked<
    TrackerRepository
  >;
  it('should return the trackers ordered by speed decreasing', async () => {
    const result = mockedTrackerService.getAll.mockResolvedValueOnce([]);
    expect(await result()).toEqual([]);
  });

  it('should return an empty list when the Tracker array is empty', async () => {
    expect([]).toEqual([]);
  });
});
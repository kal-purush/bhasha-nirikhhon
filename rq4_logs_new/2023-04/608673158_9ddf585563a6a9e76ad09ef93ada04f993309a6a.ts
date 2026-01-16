import { Event } from '../src/modules/events/event.schema';
import { EventsService } from '../src/modules/events/events.service';
import { User } from '../src/modules/users/user.schema';

describe('EventService', () => {
  let eventService: EventsService;
  let mockEventModel: any;

  beforeEach(async () => {
    mockEventModel = {
      find: jest.fn(),
    };

    eventService = new EventsService(mockEventModel);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });
  describe('getPreviousEventsOfEvent', () => {
    it('should return an empty array if there are no previous events', async () => {
      const mockUser = new User({
        email: 'test@test.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password',
      });

      const mockEvent = new Event({
        user: mockUser,
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });
      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([]),
        }),
      });

      const result = await eventService.getPreviousEventsOfEvent(mockEvent);

      expect(result).toEqual([]);
    });

    it('should return an array of events that occur before the given event', async () => {
      const mockUser = new User({
        email: 'test@test.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password',
      });

      const mockEvent = new Event({
        user: mockUser,
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });

      const mockPrevEvent = new Event({
        user: mockUser,
        start_date: new Date('2022-04-19T14:00:00.000Z'),
        end_date: new Date('2022-04-19T15:00:00.000Z'),
      });
      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([mockPrevEvent]),
        }),
      } as any);

      jest.spyOn(eventService, 'getSimultaneousEventsOfEvent').mockResolvedValueOnce([]);

      const result = await eventService.getPreviousEventsOfEvent(mockEvent);
      expect(result).toEqual([mockPrevEvent]);
    });

    it('should not return the given event in the array', async () => {
      const user = new User({
        email: 'test@example.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password123',
      });
      const mockEvent = new Event({
        user,
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });
      const mockPrevEvent = new Event({
        user,
        start_date: new Date('2022-04-19T14:00:00.000Z'),
        end_date: new Date('2022-04-19T15:00:00.000Z'),
      });
      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([mockPrevEvent, mockEvent]),
        }),
      } as any);

      const result = await eventService.getPreviousEventsOfEvent(mockEvent);
      expect(result).not.toContain(mockEvent);
    });
    // write more tests here for different cases
  });

  describe('getNextEventsOfEvent', () => {
    it('should return the next event if there is one', async () => {
      const mockEvent = new Event({
        user: new User({
          email: 'test@test.com',
          firstName: 'John',
          lastName: 'Doe',
          passwordHash: 'testpasswordhash',
        }),
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });
      const mockNextEvent = new Event({
        user: new User({
          email: 'test@test.com',
          firstName: 'John',
          lastName: 'Doe',
          passwordHash: 'testpasswordhash',
        }),
        start_date: new Date('2022-04-20T15:00:00.000Z'),
        end_date: new Date('2022-04-20T16:00:00.000Z'),
      });
      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([mockNextEvent]),
        }),
      });
      jest.spyOn(eventService, 'getSimultaneousEventsOfEvent').mockResolvedValueOnce([]);
      const result = await eventService.getNextEventsOfEvent(mockEvent);
      expect(result).toEqual([mockNextEvent]);
    });
    it('should return an empty array if the given event is the first event', async () => {
      const mockUser = new User({
        email: 'test@test.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password',
      });

      const mockEvent = new Event({
        user: mockUser,
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });

      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([]),
        }),
      } as any);

      const previousEvents = await eventService.getPreviousEventsOfEvent(mockEvent);

      expect(previousEvents).toEqual([]);
    });
  });

  describe('getSimultaneousEventsOfEvent', () => {
    it('should return an array of simultaneous events', async () => {
      const user = new User({
        email: 'test@test.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password',
      });

      const event1 = new Event({
        user,
        start_date: new Date('2022-04-20T12:00:00.000Z'),
        end_date: new Date('2022-04-20T13:00:00.000Z'),
      });
      const event2 = new Event({
        user,
        start_date: new Date('2022-04-20T12:30:00.000Z'),
        end_date: new Date('2022-04-20T13:30:00.000Z'),
      });
      const event3 = new Event({
        user,
        start_date: new Date('2022-04-20T13:30:00.000Z'),
        end_date: new Date('2022-04-20T14:30:00.000Z'),
      });
      const event4 = new Event({
        user,
        start_date: new Date('2022-04-20T14:00:00.000Z'),
        end_date: new Date('2022-04-20T15:00:00.000Z'),
      });

      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([event1, event2, event3, event4]),
        }),
      } as any);

      const simultaneousEvents = await eventService.getSimultaneousEventsOfEvent(event2);

      expect(simultaneousEvents).toEqual([event1, event2, event3]);
    });

    it('should return an empty array if there are no simultaneous events', async () => {
      const user = new User({
        email: 'test@test.com',
        firstName: 'John',
        lastName: 'Doe',
        passwordHash: 'password',
      });

      const event = new Event({
        user,
        start_date: new Date('2022-04-20T12:00:00.000Z'),
        end_date: new Date('2022-04-20T13:00:00.000Z'),
      });

      jest.spyOn(mockEventModel, 'find').mockReturnValueOnce({
        sort: jest.fn().mockReturnValueOnce({
          limit: jest.fn().mockResolvedValueOnce([event]),
        }),
      } as any);

      const simultaneousEvents = await eventService.getSimultaneousEventsOfEvent(event);

      expect(simultaneousEvents).toEqual([]);
    });
  });
});
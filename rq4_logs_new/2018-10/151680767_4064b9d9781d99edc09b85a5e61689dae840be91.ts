const Botmock = require('botkit-mock'); // TODO: use @types/botkit-mock
import { ping } from './ping';

describe('slack', () => {
  const baseMessage = {
    user: 'sushi',
    channel: 'someChannel',
  };
  let mockController: any; // TODO: more strict type
  let bot: any; // TODO: TODO: more strict type

  beforeEach(() => {
    mockController = Botmock({ stats_optout: true, debug: false });
    bot = mockController.spawn({ type: 'slack' });
  });

  afterEach(() => {
    mockController.shutdown();
  });

  describe('ping', () => {
    beforeEach(() => ping(mockController as any));

    it('should return `pong` if user types `ping`', async () =>
      await bot
        .usersInput([
          {
            ...baseMessage,
            messages: [{ text: 'ping', isAssertion: true }],
          },
        ])
        .then((message: any) => expect(message.text).toBe('pong')));

    it('should return `pong` if user types `Ping`', async () =>
      await bot
        .usersInput([
          {
            ...baseMessage,
            messages: [{ text: 'Ping', isAssertion: true }],
          },
        ])
        .then((message: any) => expect(message.text).toBe('pong')));

    it('should not return if user types invalid word', async () =>
      await bot
        .usersInput([
          {
            ...baseMessage,
            messages: [{ text: 'shopping', isAssertion: true }],
          },
        ])
        .then((message: any) => expect(message.text).toBe(undefined)));
  });
});
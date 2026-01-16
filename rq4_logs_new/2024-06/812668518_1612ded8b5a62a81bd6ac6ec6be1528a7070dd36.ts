import { createThread } from '../../../../common/interaction';
import { execute } from '../start';

jest.mock('../../../../common/interaction', () => ({
  createThread: jest.fn(),
}));
jest.mock('../../../../common/random', () => ({
  getRandOption: jest.fn().mockImplementation((options) => options[0]),
}));

describe('start', () => {
  const mockCreateThread = createThread as jest.Mock;
  const mockReply = jest.fn();
  afterEach(() => {
    mockReply.mockReset();
  });
  describe('when the thread is successfully created', () => {
    beforeEach(async () => {
      mockCreateThread.mockReturnValue({ isSuccess: true, threadId: '' });
      await execute({ reply: mockReply } as any);
    });
    it('then it replies with a startup message', () => {
      expect(mockReply).toHaveBeenCalledWith('Booting up...');
    });
  });
  describe('when the thread create has some error', () => {
    beforeEach(async () => {
      mockCreateThread.mockReturnValue({ isSuccess: false, threadId: '' });
      await execute({ reply: mockReply } as any);
    });
    it('then it replies with a snarky message', () => {
      expect(mockReply).toHaveBeenCalledWith(`bro I can't create a thread from a thread :unamused:`);
    });
  });
});
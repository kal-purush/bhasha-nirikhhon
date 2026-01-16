import { Logger } from '.';
import { ILogger } from './types';

describe('Logger', () => {
  let logger: ILogger;
  const mockConsoleLog = jest.spyOn(console, 'log');
  mockConsoleLog.mockImplementation(() => {});

  beforeEach(() => {
    logger = new Logger('testTarget');
    mockConsoleLog.mockClear();
  });

  it('log with the correct target and name', () => {
    const name = 'testName';
    logger.log(name);

    expect(mockConsoleLog).toHaveBeenCalledWith(
      { target: 'testTarget', name: 'testName' },
      {}
    );
  });

  it('log with the correct arguments', () => {
    const name = 'testName';
    const args = [123, 'abc'];
    logger.log(name, ...args);

    expect(mockConsoleLog).toHaveBeenCalledWith(
      { target: 'testTarget', name: 'testName' },
      { '0': 123, '1': 'abc' }
    );
  });
});
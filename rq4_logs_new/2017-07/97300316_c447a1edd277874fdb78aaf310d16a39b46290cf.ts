import { handleCommand, emptyExecutor } from './command-handler';
import { ListLocales } from './list-locales/ListLocales';

describe('handleCommand', () => {
  it('should return command for name if command exists', () => {
    const handler = handleCommand('ListLocales');

    expect(handler).toBeInstanceOf(Function);
    expect(handler).not.toBe(emptyExecutor);
  });

  it('should return command for name if command not exists', () => {
    const handler = handleCommand('UnesistingCommand');
    expect(handler).toBe(emptyExecutor);
  });
});
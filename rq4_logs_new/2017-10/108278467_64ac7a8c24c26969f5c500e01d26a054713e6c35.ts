import { KEY_A, KEY_DOWN } from 'uinput';
import { getCommandRegex, getCreateOptions, getSetupOptions, toKey } from '../src/commands';

describe('Utility functions', () => {
  it('should test true for commands', () => {
    const regex = getCommandRegex();
    expect(regex.test('a')).toBe(true);
    expect(regex.test('x')).toBe(true);
    expect(regex.test('start')).toBe(true);
    expect(regex.test('down')).toBe(true);
    expect(regex.test('l2')).toBe(true);
    expect(regex.test('admin_end_game')).toBe(true);
  });

  it('should test false for other texts', () => {
    const regex = getCommandRegex();
    expect(regex.test('ab')).toBe(false);
    expect(regex.test('z')).toBe(false);
    expect(regex.test('s')).toBe(false);
    expect(regex.test('seelect')).toBe(false);
    expect(regex.test('1l2')).toBe(false);
    expect(regex.test('admin-end-game')).toBe(false);
  });

  it('should return the setup options', () => {
    const options = getSetupOptions();
    expect(options.EV_KEY).toContain(KEY_A);
  });

  it('should return the create options', () => {
    const options = getCreateOptions();
    expect(options.id).toHaveProperty('bustype');
    expect(options.id).toHaveProperty('product');
  });

  it('toKey', () => {
    expect(toKey('a')).toContain(KEY_A);
    expect(toKey('down')).toContain(KEY_DOWN);
  });
});
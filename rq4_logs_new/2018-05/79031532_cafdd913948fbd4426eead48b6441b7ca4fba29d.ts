import placeholder from "./placeholder";
import Config from "../Config";

describe('placeholder(): Middleware', () => {
  it('should return a value', done => {
    const middleware = placeholder();
    const config = new Config([]);

    const next = jest.fn(done);
    middleware(config, 'test', 'foo', next);

    expect(next.mock.calls[0][1]).toBe('foo');
  });

  it('should resolve a placeholder', done => {
    const middleware = placeholder();
    const config = new Config([]);
    config.get = jest.fn(() => 'BAR');

    const next = jest.fn(done);
    middleware(config, 'test', 'foo${bar}baz', next);

    expect(next.mock.calls[0][1]).toBe('fooBARbaz');
  });

  it('should resolve a placeholder using default value', done => {
    const middleware = placeholder();
    const config = new Config([]);

    const next = jest.fn(done);
    middleware(config, 'test', 'foo${bar:BAR}baz', next);

    expect(next.mock.calls[0][1]).toBe('fooBARbaz');
  });
});
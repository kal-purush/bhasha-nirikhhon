import Foo from './Foo';

describe('Foo', () => {
  it('should return "Hello, World!"', () => {
    const foo = new Foo();
    expect(foo.bar()).toEqual('Hello, World!');
  });
});
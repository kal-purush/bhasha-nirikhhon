import getInitialStatePlugin from './getInitialStatePlugin';

describe('getInitialStatePlugin', () => {
  it('appends result of getInitialState to initialState', () => {
    const newModule = getInitialStatePlugin({
      name: 'Test',
      reducers: {},
      getInitialState: () => ({ example: 'Value' })
    });
    expect(newModule.initialState).toEqual({ example: 'Value' });
  });

  it('getInitialState wins in case of conflicts', () => {
    const newModule = getInitialStatePlugin({
      name: 'Test',
      reducers: {},
      initialState: { example: 'Bad Value' },
      getInitialState: () => ({ example: 'Good Value' })
    });
    expect(newModule.initialState).toEqual({ example: 'Good Value' });
  });

  it('doesnt explode when getInitialState missing', () => {
    const newModule = getInitialStatePlugin({
      name: 'Test',
      reducers: {},
      initialState: { example: 'Bad Value' }
    });
    expect(newModule.initialState).toEqual({ example: 'Bad Value' });
  });
});
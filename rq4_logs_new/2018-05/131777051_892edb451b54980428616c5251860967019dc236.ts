import React from 'react';
import * as messageContext from './message-context';

describe('messageContext', () => {
  it('should export Consumer', () => {
    expect(messageContext).toHaveProperty('Consumer');
  });

  it('should export Provider', () => {
    expect(messageContext).toHaveProperty('Provider');
  });

  it('should export object as default', () => {
    expect(messageContext.default).toBeInstanceOf(Object);
  });
});
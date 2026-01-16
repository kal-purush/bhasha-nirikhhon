import { BoldDirective } from './bold.directive';

describe('SharedDirective', () => {
  it('should create an instance', () => {
    // @ts-ignore
    const directive = new BoldDirective();
    expect(directive).toBeTruthy();
  });
});
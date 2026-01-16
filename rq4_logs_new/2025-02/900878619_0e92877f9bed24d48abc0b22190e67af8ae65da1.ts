import '@testing-library/jest-dom';

import MatchMediaMock from 'jest-matchmedia-mock';

new MatchMediaMock();

global.DOMRect = class DOMRect {
  constructor(
    public x = 0,
    public y = 0,
    public width = 0,
    public height = 0,
    public top = 0,
    public right = 0,
    public bottom = 0,
    public left = 0
  ) {}

  // Mock a static method `fromRect`
  static fromRect(rectangle = { x: 0, y: 0, width: 0, height: 0 }) {
    return new DOMRect(
      rectangle.x,
      rectangle.y,
      rectangle.width,
      rectangle.height,
      0,
      0,
      0,
      0
    );
  }

  toJSON() {
    return {
      x: this.x,
      y: this.y,
      width: this.width,
      height: this.height,
      top: this.top,
      right: this.right,
      bottom: this.bottom,
      left: this.left,
    };
  }
};

class ResizeObserverMock {
  observe() {}

  unobserve() {}

  disconnect() {}
}

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error
global.ResizeObserver = ResizeObserverMock as unknown as ResizeObserver;
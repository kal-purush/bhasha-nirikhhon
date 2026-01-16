import { Reflector } from '@nestjs/core';
import {
  EVENT_HANDLER_KEY,
  UseEventHandler,
} from './use-event-handler.decorator.js';

describe('UseEventHandler', () => {
  @UseEventHandler('eventHandler1')
  class TestController {
    async test() {
      return;
    }

    @UseEventHandler('eventHandler2')
    async otherRoute() {
      return;
    }
  }

  let reflector: Reflector;

  beforeAll(() => {
    reflector = new Reflector();
  });

  it('should decorate a controller', () => {
    const controller = new TestController();

    const actualRequirements = reflector.getAllAndOverride<string>(
      EVENT_HANDLER_KEY,
      [controller.test, controller.constructor],
    );

    expect(actualRequirements).toEqual('eventHandler1');
  });

  it('should decorate a controller route', () => {
    const controller = new TestController();

    const actualRequirements = reflector.getAllAndOverride<string>(
      EVENT_HANDLER_KEY,
      [controller.otherRoute, controller.constructor],
    );

    expect(actualRequirements).toEqual('eventHandler2');
  });
});
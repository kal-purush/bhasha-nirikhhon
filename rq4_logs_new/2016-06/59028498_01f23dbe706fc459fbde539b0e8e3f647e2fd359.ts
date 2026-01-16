import { Injectable } from '@angular/core';
import {
  Server,
  RouteBase,
  AbstractController,
  Action,
  Before,
  After,
  debugLog
} from '@ubiquits/core/server';
import { Logger } from '@ubiquits/core/common';

// @BeforeAll(log('one'), log('two'))
// @AfterAll(log('six'))
@Injectable()
@RouteBase('middleware')
export class MiddlewareController extends AbstractController {

  constructor(server: Server, logger: Logger) {
    super(server, logger);
  }

  @Action('GET', '/example')
  @Before(debugLog('three'))
  @After(debugLog('five'))
  public exampleMethod() {
    this.logger.debug('four');
    return 'banana';
  }

}
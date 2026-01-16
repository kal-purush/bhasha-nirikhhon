import {
  it,
  iit,
  describe,
  ddescribe,
  expect,
  inject,
  injectAsync,
  TestComponentBuilder,
  beforeEachProviders
} from 'angular2/testing';
import {provide} from 'angular2/angular2';
import {TodoService} from './todo-service';


describe('TodoService Service', () => {

  beforeEachProviders(() => []);


  it('should ...', inject([TodoService], (service:TodoService) => {

  }));

});
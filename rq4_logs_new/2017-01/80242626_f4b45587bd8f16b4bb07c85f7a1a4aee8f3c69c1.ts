import { FormsModule } from '@angular/forms';
import {
  async,
  TestBed
 } from '@angular/core/testing';

import { Observable } from 'rxjs/Observable';

import { LoginComponent } from './login.component';
import { NameListService } from '../shared/name-list/name-list.service';

export function main() {
  describe('Login component', () => {

    beforeEach(() => {

      TestBed.configureTestingModule({
        imports: [FormsModule],
        declarations: [LoginComponent],
        providers: [
          { provide: NameListService, useValue: new MockNameListService() }
        ]
      });

    });

    it('should work',
      async(() => {
        TestBed
          .compileComponents()
          .then(() => {
            let fixture = TestBed.createComponent(LoginComponent);
            let loginInstance = fixture.debugElement.componentInstance;
            let loginDOMEl = fixture.debugElement.nativeElement;
            let mockNameListService = <MockNameListService>fixture.debugElement.injector.get(NameListService);
            let nameListServiceSpy = spyOn(mockNameListService, 'get').and.callThrough();

            mockNameListService.returnValue = ['1', '2', '3'];

            fixture.detectChanges();

            expect(loginInstance.nameListService).toEqual(jasmine.any(MockNameListService));
            expect(loginDOMEl.querySelectorAll('li').length).toEqual(3);
            expect(nameListServiceSpy.calls.count()).toBe(1);

            loginInstance.newName = 'Minko';
            loginInstance.addName();

            fixture.detectChanges();

            expect(loginDOMEl.querySelectorAll('li').length).toEqual(4);
            expect(loginDOMEl.querySelectorAll('li')[3].textContent).toEqual('Minko');
          });

      }));
  });
}

class MockNameListService {

  returnValue: string[];

  get(): Observable<string[]> {
    return Observable.create((observer: any) => {
      observer.next(this.returnValue);
      observer.complete();
    });
  }
}
/* tslint:disable */
import { ApplicationRef, Component } from '@angular/core';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';

import { By } from '@angular/platform-browser';
import { SignUpComponent } from "./signup";
import { UserApi } from '../../../shared/services';
import { SharedModule } from '../../../shared';
import reducers from '../../../core/reducers';
import { StoreModule } from '@ngrx/store';

import { APP_CORE_API_PROVIDERS } from '../../../core';
import { Router, RouterModule } from '@angular/router';
import { Subject, Observable } from 'rxjs';
import { TestBed, async, inject } from '@angular/core/testing';
import { Store } from '@ngrx/store';

@Component({
    selector: 'test-cmp',
    template: ''
})
class TestComponent { }
class MockRouter {
    navigate(value) { }
}
describe('COMPONENTS TESTS', () => {
    describe("Quich search component tests", () => {
        beforeEach(() => TestBed.configureTestingModule({
            imports: [
                RouterModule,
                FormsModule,
                ReactiveFormsModule,
                StoreModule.provideStore(reducers),
                // import Shared module
                SharedModule
            ],
            providers: [
                { provide: Router, useClass: MockRouter },
                ...APP_CORE_API_PROVIDERS
            ],
            declarations: [
                TestComponent,
                SignUpComponent
            ]
        }));

        beforeEach(() => {
            TestBed.overrideComponent(TestComponent, {
                set: {
                    template: '<signup></signup>'
                }
            });
        });

        beforeEach(inject([Router, UserApi, Store], (router, userApi) => {
            spyOn(router, "navigate");
            spyOn(userApi, "signup").and.returnValue(Observable.of({ count: 42 }));

        }));

        it('should init', async(() => {
            TestBed.compileComponents().then(() => {
                let fixture = TestBed.createComponent(TestComponent);
                fixture.detectChanges();
                let compiled = fixture.debugElement.nativeElement;
                expect(compiled.innerHTML).toContain('Username');
            });
        }));
    });
});
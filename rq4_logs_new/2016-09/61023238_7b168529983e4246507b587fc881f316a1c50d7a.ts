/* tslint:disable */
import { ApplicationRef, Component } from '@angular/core';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';

import { By } from '@angular/platform-browser';
import { SignInComponent } from "./signIn";
import { UserApi } from '../../../shared/services';
import { LoopBackAuth } from '../../../core';
import { Router, RouterModule } from '@angular/router';
import { Subject, Observable } from 'rxjs';
import { TestBed, async, inject, ComponentFixture } from '@angular/core/testing'
import { SharedModule } from '../../../shared';
import { APP_CORE_API_PROVIDERS } from '../../../core';

let fixture: ComponentFixture<SignInComponent>,
    comp: SignInComponent,
    userApiService: UserApi;

describe('COMPONENTS TESTS', () => {
    class MockUserApiService {
        public login(credentials: any, include: any = 'user') {
            return Observable.of({});
        }
    }
    class MockRouter {
        navigate(value) { }
    }

    class MockLoopBackAuth {
    }

    describe('LOGIN COMPONENT TESTS', () => {
        beforeEach(() => TestBed.configureTestingModule({
            declarations: [SignInComponent],
            imports: [
                // RouterModule,
                SharedModule
            ],
            providers: [
                ...APP_CORE_API_PROVIDERS,
                { provide: UserApi, useValue: new MockUserApiService() },
                { provide: Router, useClass: MockRouter },
                { provide: LoopBackAuth, useClass: MockLoopBackAuth },
            ]
        }));

        beforeEach(async(() => {
            TestBed.compileComponents();
            fixture = TestBed.createComponent(SignInComponent);
            comp = fixture.componentInstance;
            userApiService = fixture.debugElement.injector.get(UserApi);          
            spyOn(userApiService, 'login').and.returnValue(Observable.of({ result: 'ok' }));
        }));
        it('true is true', async(() => {
            expect(true).toEqual(true)
        }));
    })
})
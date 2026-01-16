import { TestBed, ComponentFixture } from '@angular/core/testing';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { RouterModule } from '@angular/router';
import { Type } from '@angular/core';


import { TestHttpHelper } from '../../../test_helpers';
import { DraalFormsModule, DraalWidgetsCoreModule } from '../../widgets';
import { DraalServicesModule, NetworkService, ApiService } from '../../services';
import { APP_BASE_HREF } from '@angular/common';

import { RegisterComponent } from './registration';
import { LoginComponent } from './login';
import { LogoutComponent } from './logout';
import { ActivateComponent } from './activate';
import { PwResetRequestComponent } from './pwreset';


export class AuthTestingModule {
    testBed: typeof TestBed;

    constructor() {}

    static get TestBed(): typeof TestBed {
        return TestBed;
    }

    init(componentProviders: Array<any>): Promise<any> {
        const providers = [
            NetworkService,
            ApiService,
            {provide: APP_BASE_HREF, useValue: ''}
        ];

        componentProviders.forEach((provider) => {
            providers.push(provider);
        });

        this.testBed = TestBed.configureTestingModule({
            imports: [
                NgbModule.forRoot(),
                DraalFormsModule,
                DraalWidgetsCoreModule,
                DraalServicesModule.forRoot(),
                RouterModule.forRoot([])
            ].concat(TestHttpHelper.http),
            declarations: [
                LoginComponent,
                LogoutComponent,
                ActivateComponent,
                RegisterComponent,
                PwResetRequestComponent
            ],
            providers
        });

        return this.testBed.compileComponents();
    }

    getComponent(component: Type<any>): ComponentFixture<any> {
        return TestBed.createComponent(component);
    }
}
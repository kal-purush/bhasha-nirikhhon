/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';
// USE TO NOT TEST ROUTER
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { AbstractMockObservableService } from '../../testing/ObService';
import { LawAdminService } from './admin-law.service';
import { Router } from '@angular/router';

import { AdminLawComponent } from './admin-law.component';
import { Observable } from 'rxjs/Observable';


describe('AdminLawComponent', () => {
  let component: AdminLawComponent;
  let fixture: ComponentFixture<AdminLawComponent>;

  class RouterStub {
    navigateByUrl(url: string) { return url; }
  }

  class LawAdminServiceStub extends LawAdminService {
    constructor() {
      super();
    }
    tab$: Observable<string>;
    addTab(route: string) {
      return route;
    }
  }

  beforeEach(async(() => {
    let lawAdminServiceStub = new LawAdminServiceStub();
    TestBed.configureTestingModule({
      declarations: [AdminLawComponent],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        { provide: Router, useClass: RouterStub, },
        { provide: LawAdminService, useValue: lawAdminServiceStub },
      ],
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminLawComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', async(() => {
    expect(component).toBeTruthy();
  }));
});
import { DebugElement } from '@angular/core';
import { UtilService } from '@core/services/util.service';
import { MdbModalModule, MdbModalService } from 'mdb-angular-ui-kit/modal';
import { HeaderComponent } from './header.component';
import { CreateComponent } from '../../../modules/customers/create/create.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserModule, By } from '@angular/platform-browser';
import { MDBBootstrapModule } from 'angular-bootstrap-md';
import { MockDependenciesModule } from 'app/modules/mock-dependecies/mock-dependencies.module';
import { MdbFormsModule } from 'mdb-angular-ui-kit/forms';
import { ToastrModule, ToastrService } from 'ngx-toastr';

describe('HeaderComponent', () => {
  let component: HeaderComponent;
  let fixture: ComponentFixture<HeaderComponent>;
  let debugElement: DebugElement;
  let utilService: UtilService;
  let customerModalSPy: any;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [HeaderComponent, CreateComponent],
      imports: [
        HttpClientModule,
        ToastrModule.forRoot(),
        MdbModalModule,
        CommonModule,
        BrowserModule,
        MDBBootstrapModule.forRoot(),
        FormsModule,
        ReactiveFormsModule,
        MdbFormsModule,
        MockDependenciesModule,
      ],
      providers: [ToastrService, MdbModalService],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HeaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    utilService = TestBed.inject(UtilService);
    debugElement = fixture.debugElement;
    customerModalSPy = spyOn(component, 'addCustomer').and.callThrough();
  });

  it('should call addCustomer() function on menu link click', () => {
    expect(utilService.activeModalRef).toBeFalsy();
    debugElement
      .query(By.css('a.create-customer'))
      .triggerEventHandler('click', null);
    fixture.detectChanges();
    expect(customerModalSPy).toHaveBeenCalled();
  });

  it('should open create customer modal on add customer menu link click', () => {
    expect(component.modalRef).toBeFalsy();
    debugElement
      .query(By.css('a.create-customer'))
      .triggerEventHandler('click', null);
    fixture.detectChanges();
    expect(component.modalRef).toBeTruthy();
  });
});
// import { AppComponent } from './app.component';
// import { MockBackend } from '@angular/http/testing';
// import { inject, async, ComponentFixture, TestBed } from '@angular/core/testing';
// import { DebugElement, Component } from '@angular/core';
// import { BaseRequestOptions, Http, HttpModule, XHRBackend } from '@angular/http';
// import { FormsModule } from '@angular/forms';
// import { Observable } from 'rxjs';
// import { Purchase } from './purchase';
// import { PurchaseService } from './purchase.service';
// import { By } from '@angular/platform-browser';
// import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';


// let MockPurchase: Purchase = <Purchase>{ id: 1, name: 'John Do', creditCardNumber: 111, expiry: 1111 };

// describe('Component: HeroDetail', () => {
//   let fixture: ComponentFixture<AppComponent>;
//   let purchaseService: PurchaseService;
//   // let mockBackend: MockBackend;
//    let XHRBackend: XHRBackend;
//   let appComponent: AppComponent;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         PurchaseService,
//         { provide: XHRBackend, useClass: MockBackend }        // BaseRequestOptions,
//         // {
//         //   provide: Http,
//         //   useFactory: (backend: MockBackend, options: BaseRequestOptions) => new Http(backend, options),
//         //   deps: [MockBackend, BaseRequestOptions]
//         // }
//       ],
//       declarations: [
//         AppComponent
//       ],
//       imports: [
//         FormsModule,
//         HttpModule
//       ],
//       schemas: [CUSTOM_ELEMENTS_SCHEMA]
//     });

//     fixture = TestBed.createComponent(AppComponent)


//   });

//   beforeEach(inject([PurchaseService, XHRBackend],
//     (ps: PurchaseService, mb: XHRBackend) => {
//       purchaseService = ps;
//       XHRBackend = mb;
//     }));

//   describe('Component: AppComponent', () => {
//     it('should have a disabled purchase button when form is invalid', () => {
//       // should I inject the form??
//       const purchaseButton = fixture.debugElement.query(By.css('button[disabled="true"]'));
//       expect(purchaseButton).toBeTruthy();
//     });

//     it('should submit a purchse when form is valid', () => {
//       this.purchase = '';
//       fixture.detectChanges();
//       const purchaseButton = fixture.debugElement.query(By.css('button[disabled="false"]'));
//       expect(purchaseButton).toBeTruthy();
//     });

//   });
// });
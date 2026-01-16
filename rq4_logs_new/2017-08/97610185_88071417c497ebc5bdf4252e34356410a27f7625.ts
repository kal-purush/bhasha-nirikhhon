import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SaleDocumentClientComponent } from './sale-document-client.component';

describe('SaleDocumentClientComponent', () => {
  let component: SaleDocumentClientComponent;
  let fixture: ComponentFixture<SaleDocumentClientComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SaleDocumentClientComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SaleDocumentClientComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
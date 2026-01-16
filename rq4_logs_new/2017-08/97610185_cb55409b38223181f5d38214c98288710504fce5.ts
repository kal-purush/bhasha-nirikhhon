import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SaleDocumentBodyComponent } from './sale-document-body.component';

describe('SaleDocumentBodyComponent', () => {
  let component: SaleDocumentBodyComponent;
  let fixture: ComponentFixture<SaleDocumentBodyComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SaleDocumentBodyComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SaleDocumentBodyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
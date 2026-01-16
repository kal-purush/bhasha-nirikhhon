import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SaleDocumentFooterComponent } from './sale-document-footer.component';

describe('SaleDocumentFooterComponent', () => {
  let component: SaleDocumentFooterComponent;
  let fixture: ComponentFixture<SaleDocumentFooterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SaleDocumentFooterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SaleDocumentFooterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
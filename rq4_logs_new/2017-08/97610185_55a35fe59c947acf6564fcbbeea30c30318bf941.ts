import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DocProductsListComponent } from './doc-products-list.component';

describe('DocProductsListComponent', () => {
  let component: DocProductsListComponent;
  let fixture: ComponentFixture<DocProductsListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DocProductsListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DocProductsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
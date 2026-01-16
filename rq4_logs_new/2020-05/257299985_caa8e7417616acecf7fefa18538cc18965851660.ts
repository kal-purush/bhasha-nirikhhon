import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListCustonerComponent } from './list-custoner.component';

describe('ListCustonerComponent', () => {
  let component: ListCustonerComponent;
  let fixture: ComponentFixture<ListCustonerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListCustonerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListCustonerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
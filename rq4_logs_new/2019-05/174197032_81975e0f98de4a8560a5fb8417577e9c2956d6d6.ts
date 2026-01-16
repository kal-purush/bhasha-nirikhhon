import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ListallservicesComponent } from './listallservices.component';

describe('ListallservicesComponent', () => {
  let component: ListallservicesComponent;
  let fixture: ComponentFixture<ListallservicesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ListallservicesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ListallservicesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
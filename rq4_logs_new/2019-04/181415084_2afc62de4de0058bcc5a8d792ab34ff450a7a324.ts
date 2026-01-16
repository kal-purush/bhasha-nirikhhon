import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddInfectionComponent } from './add-infection.component';

describe('AddInfectionComponent', () => {
  let component: AddInfectionComponent;
  let fixture: ComponentFixture<AddInfectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddInfectionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddInfectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
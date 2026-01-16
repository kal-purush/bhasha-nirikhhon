import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateTestEntryComponent } from './create-test-entry.component';

describe('CreateTestEntryComponent', () => {
  let component: CreateTestEntryComponent;
  let fixture: ComponentFixture<CreateTestEntryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateTestEntryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateTestEntryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
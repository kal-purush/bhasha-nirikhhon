import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SetupStageComponent } from './setup-stage.component';

describe('SetupStageComponent', () => {
  let component: SetupStageComponent;
  let fixture: ComponentFixture<SetupStageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SetupStageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SetupStageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MbtiTestComponent } from './mbti-test.component';

describe('MbtiTestComponent', () => {
  let component: MbtiTestComponent;
  let fixture: ComponentFixture<MbtiTestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MbtiTestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MbtiTestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
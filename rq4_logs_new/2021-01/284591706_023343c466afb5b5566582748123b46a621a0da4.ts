import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TestBabylonComponent } from './test-babylon.component';

describe('TestBabylonComponent', () => {
  let component: TestBabylonComponent;
  let fixture: ComponentFixture<TestBabylonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TestBabylonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TestBabylonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
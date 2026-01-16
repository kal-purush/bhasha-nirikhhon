import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TestboxComponent } from './testbox.component';

describe('TestboxComponent', () => {
  let component: TestboxComponent;
  let fixture: ComponentFixture<TestboxComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TestboxComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TestboxComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MockFrontendComponent } from './mock-frontend.component';

describe('MockFrontendComponent', () => {
  let component: MockFrontendComponent;
  let fixture: ComponentFixture<MockFrontendComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MockFrontendComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MockFrontendComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
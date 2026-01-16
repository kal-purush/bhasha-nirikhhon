import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {ToolbarNotLoggedComponent} from './toolbar-not-logged.component';

describe('ToolbarNotLoggedComponent', () => {
  let component: ToolbarNotLoggedComponent;
  let fixture: ComponentFixture<ToolbarNotLoggedComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ToolbarNotLoggedComponent]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ToolbarNotLoggedComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
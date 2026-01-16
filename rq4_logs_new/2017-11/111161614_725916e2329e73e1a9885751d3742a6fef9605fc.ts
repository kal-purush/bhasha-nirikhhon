import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsessionComponent } from './tabsession.component';

describe('TabsessionComponent', () => {
  let component: TabsessionComponent;
  let fixture: ComponentFixture<TabsessionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TabsessionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TabsessionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
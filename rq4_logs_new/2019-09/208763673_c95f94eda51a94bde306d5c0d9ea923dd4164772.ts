import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UilocationfinderComponent } from './uilocationfinder.component';

describe('UilocationfinderComponent', () => {
  let component: UilocationfinderComponent;
  let fixture: ComponentFixture<UilocationfinderComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UilocationfinderComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UilocationfinderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
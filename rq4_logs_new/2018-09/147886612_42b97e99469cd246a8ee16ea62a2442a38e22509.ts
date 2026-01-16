import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComponentProfileComponent } from './component-profile.component';

describe('ComponentProfileComponent', () => {
  let component: ComponentProfileComponent;
  let fixture: ComponentFixture<ComponentProfileComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComponentProfileComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComponentProfileComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
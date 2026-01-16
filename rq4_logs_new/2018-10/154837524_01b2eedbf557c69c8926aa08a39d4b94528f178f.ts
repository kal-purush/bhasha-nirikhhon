import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RoleChooserComponent } from './role-chooser.component';

describe('RoleChooserComponent', () => {
  let component: RoleChooserComponent;
  let fixture: ComponentFixture<RoleChooserComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RoleChooserComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RoleChooserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
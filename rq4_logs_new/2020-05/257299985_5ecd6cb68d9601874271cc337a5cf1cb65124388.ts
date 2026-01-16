import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AdminSlideBarComponent } from './admin-slide-bar.component';

describe('AdminSlideBarComponent', () => {
  let component: AdminSlideBarComponent;
  let fixture: ComponentFixture<AdminSlideBarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AdminSlideBarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminSlideBarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
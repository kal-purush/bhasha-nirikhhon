import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FkSidenavComponent } from './fk-sidenav.component';

describe('FkSidenavComponent', () => {
  let component: FkSidenavComponent;
  let fixture: ComponentFixture<FkSidenavComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FkSidenavComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FkSidenavComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
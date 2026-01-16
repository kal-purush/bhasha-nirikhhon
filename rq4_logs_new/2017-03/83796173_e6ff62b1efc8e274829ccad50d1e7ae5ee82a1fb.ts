import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FkNavbarComponent } from './fk-navbar.component';

describe('FkNavbarComponent', () => {
  let component: FkNavbarComponent;
  let fixture: ComponentFixture<FkNavbarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FkNavbarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FkNavbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
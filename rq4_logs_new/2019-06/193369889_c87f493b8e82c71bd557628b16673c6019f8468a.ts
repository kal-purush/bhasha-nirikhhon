import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AuthRegComponent } from './auth-reg.component';

describe('AuthRegComponent', () => {
  let component: AuthRegComponent;
  let fixture: ComponentFixture<AuthRegComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AuthRegComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AuthRegComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AuthdocComponent } from './authdoc.component';

describe('AuthdocComponent', () => {
  let component: AuthdocComponent;
  let fixture: ComponentFixture<AuthdocComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AuthdocComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AuthdocComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NoAuthPageComponent } from './no-auth-page.component';

describe('NoAuthPageComponent', () => {
  let component: NoAuthPageComponent;
  let fixture: ComponentFixture<NoAuthPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NoAuthPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NoAuthPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
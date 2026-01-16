import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ValidCredentialsComponent } from './valid-credentials.component';

describe('ValidCredentialsComponent', () => {
  let component: ValidCredentialsComponent;
  let fixture: ComponentFixture<ValidCredentialsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ValidCredentialsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ValidCredentialsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
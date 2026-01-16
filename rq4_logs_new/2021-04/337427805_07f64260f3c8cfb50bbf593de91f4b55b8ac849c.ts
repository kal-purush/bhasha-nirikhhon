import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LoadingAnimComponent } from './loading-anim.component';

describe('LoadingAnimComponent', () => {
  let component: LoadingAnimComponent;
  let fixture: ComponentFixture<LoadingAnimComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LoadingAnimComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadingAnimComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
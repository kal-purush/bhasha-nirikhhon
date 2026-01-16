import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BocoSnackbarComponent } from './boco-snackbar.component';

describe('BocoSnackbarComponent', () => {
  let component: BocoSnackbarComponent;
  let fixture: ComponentFixture<BocoSnackbarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BocoSnackbarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BocoSnackbarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
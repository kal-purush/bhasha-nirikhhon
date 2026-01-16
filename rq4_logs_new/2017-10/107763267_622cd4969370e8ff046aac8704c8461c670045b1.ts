import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AesComponent } from './aes.component';

describe('AesComponent', () => {
  let component: AesComponent;
  let fixture: ComponentFixture<AesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
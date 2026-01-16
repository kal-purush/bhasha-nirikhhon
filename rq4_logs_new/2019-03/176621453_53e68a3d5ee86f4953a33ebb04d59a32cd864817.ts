import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgxViaCepComponent } from './ngx-via-cep.component';

describe('NgxViaCepComponent', () => {
  let component: NgxViaCepComponent;
  let fixture: ComponentFixture<NgxViaCepComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgxViaCepComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgxViaCepComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
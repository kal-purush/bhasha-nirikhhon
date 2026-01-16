import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecebidosComponent } from './recebidos.component';

describe('RecebidosComponent', () => {
  let component: RecebidosComponent;
  let fixture: ComponentFixture<RecebidosComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RecebidosComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RecebidosComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
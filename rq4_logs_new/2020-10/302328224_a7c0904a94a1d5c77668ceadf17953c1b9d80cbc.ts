import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RisersFallersComponent } from './risers-fallers.component';

describe('RisersFallersComponent', () => {
  let component: RisersFallersComponent;
  let fixture: ComponentFixture<RisersFallersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RisersFallersComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RisersFallersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
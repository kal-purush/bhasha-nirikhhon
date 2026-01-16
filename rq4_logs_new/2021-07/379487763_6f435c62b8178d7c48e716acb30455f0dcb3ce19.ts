import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ForjComponent } from './forj.component';

describe('ForjComponent', () => {
  let component: ForjComponent;
  let fixture: ComponentFixture<ForjComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ForjComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ForjComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
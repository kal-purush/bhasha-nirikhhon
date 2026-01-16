import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GiMagusComponent } from './gi-magus.component';

describe('GiMagusComponent', () => {
  let component: GiMagusComponent;
  let fixture: ComponentFixture<GiMagusComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GiMagusComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GiMagusComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
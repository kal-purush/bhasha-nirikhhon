import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DancingManComponent } from './dancing-man.component';

describe('DancingManComponent', () => {
  let component: DancingManComponent;
  let fixture: ComponentFixture<DancingManComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DancingManComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DancingManComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
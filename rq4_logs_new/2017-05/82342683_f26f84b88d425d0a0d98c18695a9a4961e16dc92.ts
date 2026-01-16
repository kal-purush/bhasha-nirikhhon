import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { StudiesTableComponent } from './studies-table.component';

describe('StudiesTableComponent', () => {
  let component: StudiesTableComponent;
  let fixture: ComponentFixture<StudiesTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ StudiesTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(StudiesTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CampusLifeComponent } from './campus-life.component';

describe('CampusLifeComponent', () => {
  let component: CampusLifeComponent;
  let fixture: ComponentFixture<CampusLifeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CampusLifeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CampusLifeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
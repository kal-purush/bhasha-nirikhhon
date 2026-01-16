import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CursosLayoutComponent } from './cursos-layout.component';

describe('CursosLayoutComponent', () => {
  let component: CursosLayoutComponent;
  let fixture: ComponentFixture<CursosLayoutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CursosLayoutComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CursosLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
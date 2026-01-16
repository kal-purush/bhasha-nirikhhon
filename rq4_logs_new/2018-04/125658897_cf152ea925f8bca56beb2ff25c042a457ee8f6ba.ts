import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ArboledaComponent } from './arboleda.component';

describe('ArboledaComponent', () => {
  let component: ArboledaComponent;
  let fixture: ComponentFixture<ArboledaComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ArboledaComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ArboledaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ComidaDetailComponent } from './comida-detail.component';

describe('ComidaDetailComponent', () => {
  let component: ComidaDetailComponent;
  let fixture: ComponentFixture<ComidaDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ComidaDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ComidaDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NuevoReclamoComponent } from './nuevo-reclamo.component';

describe('NuevoReclamoComponent', () => {
  let component: NuevoReclamoComponent;
  let fixture: ComponentFixture<NuevoReclamoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NuevoReclamoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NuevoReclamoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
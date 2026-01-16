import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReclamoComponent } from './reclamo.component';

describe('ReclamoComponent', () => {
  let component: ReclamoComponent;
  let fixture: ComponentFixture<ReclamoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReclamoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReclamoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
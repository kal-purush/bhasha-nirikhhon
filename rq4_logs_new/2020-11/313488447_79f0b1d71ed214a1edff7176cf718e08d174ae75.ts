import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BodyNosotrosComponent } from './body-nosotros.component';

describe('BodyNosotrosComponent', () => {
  let component: BodyNosotrosComponent;
  let fixture: ComponentFixture<BodyNosotrosComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BodyNosotrosComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BodyNosotrosComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AjouterassuranceComponent } from './ajouterassurance.component';

describe('AjouterassuranceComponent', () => {
  let component: AjouterassuranceComponent;
  let fixture: ComponentFixture<AjouterassuranceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AjouterassuranceComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AjouterassuranceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
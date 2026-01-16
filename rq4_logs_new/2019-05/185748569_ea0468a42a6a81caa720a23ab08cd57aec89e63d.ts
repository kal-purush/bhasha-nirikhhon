import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BuildingTypeComponent } from './building-type.component';

describe('BuildingTypeComponent', () => {
  let component: BuildingTypeComponent;
  let fixture: ComponentFixture<BuildingTypeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BuildingTypeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BuildingTypeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
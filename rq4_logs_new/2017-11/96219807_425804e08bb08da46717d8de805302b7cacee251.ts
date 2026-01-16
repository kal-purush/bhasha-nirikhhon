import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BuildContextPropertiesComponent } from './build-context-properties.component';

describe('BuildContextPropertiesComponent', () => {
  let component: BuildContextPropertiesComponent;
  let fixture: ComponentFixture<BuildContextPropertiesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BuildContextPropertiesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BuildContextPropertiesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
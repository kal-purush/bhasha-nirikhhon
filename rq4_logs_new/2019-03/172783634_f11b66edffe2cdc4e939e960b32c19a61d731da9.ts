import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MwhDoesAThingComponent } from './mwh-does-athing.component';

describe('MwhDoesAThingComponent', () => {
  let component: MwhDoesAThingComponent;
  let fixture: ComponentFixture<MwhDoesAThingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MwhDoesAThingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MwhDoesAThingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
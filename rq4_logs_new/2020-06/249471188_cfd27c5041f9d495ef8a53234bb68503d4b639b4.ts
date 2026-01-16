import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RocksonComponent } from './rockson.component';

describe('RocksonComponent', () => {
  let component: RocksonComponent;
  let fixture: ComponentFixture<RocksonComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RocksonComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RocksonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EligiblitycalComponent } from './eligiblitycal.component';

describe('EligiblitycalComponent', () => {
  let component: EligiblitycalComponent;
  let fixture: ComponentFixture<EligiblitycalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EligiblitycalComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EligiblitycalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
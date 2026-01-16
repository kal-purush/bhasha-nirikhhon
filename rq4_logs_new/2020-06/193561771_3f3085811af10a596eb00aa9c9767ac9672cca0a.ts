import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { Component, ViewChild } from '@angular/core';
import { FormBuilder } from '@angular/forms';
import { MatFormFieldModule, MatSelectModule, MatCheckboxModule, MatButtonModule, MatIconModule } from '@angular/material';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ReactiveFormsModule } from '@angular/forms';
import { FlightTargetsFormComponent } from './flight-targets-form.component';
import { InventoryTargets } from '../store/models';

@Component({
  template: `
    <form [formGroup]="flightForm">
      <grove-flight-targets #childForm formControlName="targets" [targetOptions]="options"> </grove-flight-targets>
    </form>
  `
})
class ParentFormComponent {
  @ViewChild('childForm', { static: true }) childForm: FlightTargetsFormComponent;
  constructor(private fb: FormBuilder) {}
  flightForm = this.fb.group({ targets: [''] });
  options: InventoryTargets = {
    inventoryId: 1234,
    countries: [
      { type: 'country', code: 'US', label: 'The Us' },
      { type: 'country', code: 'CA', label: 'The Canadia' }
    ],
    episodes: [{ type: 'episode', code: 'AAAA', label: 'The Episoded' }]
  };
}

describe('FlightFormComponent', () => {
  let parent: ParentFormComponent;
  let component: FlightTargetsFormComponent;
  let fixture: ComponentFixture<ParentFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ReactiveFormsModule,
        NoopAnimationsModule,
        MatFormFieldModule,
        MatSelectModule,
        MatCheckboxModule,
        MatButtonModule,
        MatIconModule
      ],
      declarations: [ParentFormComponent, FlightTargetsFormComponent]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ParentFormComponent);
    parent = fixture.componentInstance;
    component = parent.childForm;
    fixture.detectChanges();
  });

  it('sets targets from the parent form', () => {
    expect(component.targets.length).toEqual(0);
    parent.flightForm.setValue({ targets: [{}] });
    expect(component.targets.length).toEqual(1);
    parent.flightForm.reset({ targets: [{}, {}] });
    expect(component.targets.length).toEqual(2);
  });

  it('adds targets but only emits them when they have values', () => {
    expect(parent.flightForm.value).toEqual({ targets: '' });
    expect(parent.flightForm.dirty).toEqual(false);

    component.addTarget();
    component.addTarget();
    expect(parent.flightForm.value).toEqual({ targets: [] });
    expect(component.targets.length).toEqual(2);
    expect(parent.flightForm.dirty).toEqual(true);

    component.targets.at(1).setValue({ type: 'country', code: '', exclude: false });
    expect(parent.flightForm.value).toEqual({ targets: [] });

    component.targets.at(1).setValue({ type: 'country', code: 'CA', exclude: false });
    expect(parent.flightForm.value).toEqual({ targets: [{ type: 'country', code: 'CA', exclude: false }] });
  });

  it('removes targets', () => {
    parent.flightForm.reset({
      targets: [
        { type: 'country', code: 'US', exclude: false },
        { type: 'episode', code: 'AAAA', exclude: false }
      ]
    });
    expect(parent.flightForm.dirty).toEqual(false);

    component.removeTarget(0);
    expect(parent.flightForm.value).toEqual({ targets: [{ type: 'episode', code: 'AAAA', exclude: false }] });
    expect(parent.flightForm.dirty).toEqual(true);
  });

  it('validates targets', () => {
    expect(parent.flightForm.valid).toEqual(true);

    component.addTarget();
    expect(parent.flightForm.valid).toEqual(false);

    component.targets.at(0).setValue({ type: 'country', code: '', exclude: false });
    expect(parent.flightForm.valid).toEqual(false);

    component.targets.at(0).setValue({ type: 'country', code: 'US', exclude: false });
    expect(parent.flightForm.valid).toEqual(true);
  });

  it('removes target codes when their type changes', () => {
    parent.flightForm.reset({ targets: [{ type: 'country', code: 'US', exclude: false }] });
    expect(component.codeOptions[0]).toEqual(parent.options.countries);

    component.targets.at(0).setValue({ type: 'episode', code: 'US', exclude: false });
    expect(parent.flightForm.value).toEqual({ targets: [] });
    expect(component.targets.value).toEqual([{ type: 'episode', code: '', exclude: false }]);
  });

  it('restricts target codes based on their type', () => {
    component.addTarget();
    expect(component.codeOptions[0]).toEqual([]);

    component.targets.at(0).setValue({ type: 'foobar', code: '', exclude: false });
    expect(component.codeOptions[0]).toEqual([]);

    component.targets.at(0).setValue({ type: 'country', code: '', exclude: false });
    expect(component.codeOptions[0]).toEqual(parent.options.countries);

    component.targets.at(0).setValue({ type: 'episode', code: '', exclude: false });
    expect(component.codeOptions[0]).toEqual(parent.options.episodes);
  });
});
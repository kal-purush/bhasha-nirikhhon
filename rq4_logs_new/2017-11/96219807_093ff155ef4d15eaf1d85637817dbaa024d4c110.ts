import { FormBuilder } from '@angular/forms';
import { InputPropertiesComponent } from 'app/editor/flowbster-forms/input-properties/input-properties.component';

import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { JointService } from 'app/editor/flowbster-forms/shared/joint.service';

// xdescribe('InputPropertiesComponent', () => {
//   let component: InputPropertiesComponent;
//   let fixture: ComponentFixture<InputPropertiesComponent>;

//   beforeEach(
//     async(() => {
//       TestBed.configureTestingModule({
//         declarations: [InputPropertiesComponent]
//       }).compileComponents();
//     })
//   );

//   beforeEach(() => {
//     fixture = TestBed.createComponent(InputPropertiesComponent);
//     component = fixture.componentInstance;
//     fixture.detectChanges();
//   });

//   it('should be created', () => {
//     expect(component).toBeTruthy();
//   });
// });

describe('InputPropertiesComponent Isolated', () => {
  let component: InputPropertiesComponent;

  beforeEach(() => {
    const jointSVC = new JointService();
    component = new InputPropertiesComponent(new FormBuilder(), jointSVC);
    component.ngOnInit();
  });

  it('should initialize the form when the component gets initialized', () => {
    expect(component.userform).toBeDefined();
  });

  it('should call the enable function of the format control when the collector checkbox is checked', () => {
    const formatControl = component.userform.get('format');
    component.userform.get('collector').setValue(true);
    const spy = spyOn(formatControl, 'enable');

    component.onCheckboxToggle();

    expect(spy.calls.count()).toBe(1);
  });

  it('should call the disable function of the format control when the collector checkbox is checked', () => {
    const formatControl = component.userform.get('format');
    component.userform.get('collector').setValue(false);
    const spy = spyOn(formatControl, 'disable');

    component.onCheckboxToggle();

    expect(spy.calls.count()).toBe(1);
  });
});
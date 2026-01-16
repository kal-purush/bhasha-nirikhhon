import { SelectedValueTitleDirective } from './selected-value-title.directive';
import { Component } from '@angular/core';
import { StepDescription } from './step-description';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

@Component({
  template: '<p appSelectedValueTitle [stepDescription]="levelDescription" initialValue="2"><label class="mat-slider-thumb">Directives are awesome</label></p>'
})
class TestComponent {
  public levelDescription: StepDescription = {
    step0: 'zero',
    step1: 'one',
    step2: 'two',
    step3: 'three',
    step4: 'four'
  };
}

describe('SelectedValueTitleDirective', () => {
  let component: TestComponent;
  let fixture: ComponentFixture<TestComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [
        TestComponent,
        SelectedValueTitleDirective
      ]
    });

    fixture = TestBed.createComponent(TestComponent);
    component = fixture.componentInstance;
  });

  it('should add title to the label', () => {
    fixture.detectChanges();
    const label = fixture.debugElement.query(By.css('label'));
    expect(label.nativeElement.title).toBe(component.levelDescription.step2);
  });

});
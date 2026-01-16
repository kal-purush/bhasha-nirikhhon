import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import { IconComponent } from './icon.component';

describe('IconComponent', () => {
  let component: IconComponent;
  let fixture: ComponentFixture<IconComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ IconComponent ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(IconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getter and setter color()', () => {

    it('should set color', () => {
      expect(component.color).toBeFalsy();
      component.color = 'red';
      expect(component.color).toBe('red');
      component.color = 'blue';
      expect(component.color).toBe('blue');
    });

    it('should have called _updateColor(newColor)', () => {
      const spy = spyOn(component, 'updateColor');
      const newColor = 'red';
      component.color = newColor;
      expect(spy).toHaveBeenCalledWith(newColor);
    });

  });

  describe('ngOnInit()', () => {
    it('should exist', () => {
      expect(component.ngOnInit).toBeTruthy();
    });

    it('should have called _updateFontIconClasses()', () => {
      const spy = spyOn(component, 'updateFontIconClasses');
      component.ngOnInit();
      expect(spy).toHaveBeenCalled();
    });
  });

  describe('updateFontIconClasses()', () => {

    it('should remove value of previous font-set class and add value of new font-set to class', () => {
      const classes = fixture.debugElement.classes;
      const prevFontClass = component.prevFontClass;
      expect(classes[prevFontClass]).toBeTruthy();
      component.font = 'some-font-set';
      component.updateFontIconClasses();
      expect(classes[prevFontClass]).toBeFalsy();
      expect(classes['some-font-set']).toBeTruthy();
      expect(component.prevFontClass).toBe('some-font-set');
    });

    it('should remove value of previous icon class and add value of new icon to class', () => {
      const classes = fixture.debugElement.classes;
      component.font = 'fa';
      component.icon = 'some-icon';
      component.updateFontIconClasses();
      const prevIconClass = component.prevIconClass;
      expect(classes[prevIconClass]).toBeTruthy();
      component.icon = 'alarm';
      component.updateFontIconClasses();
      expect(classes[prevIconClass]).toBeFalsy();
      expect(classes[`${component.font}-${component.icon}`]).toBeTruthy();
      expect(component.prevIconClass).toBe(`${component.font}-${component.icon}`);
    });

    it('should remove value of previous font-size and add value of font-size to class', () => {
      const classes = fixture.debugElement.classes;
      component.font = 'fa';
      component.size = 'lg';
      component.updateFontIconClasses();
      const prevSizeClass = component.prevSizeClass;
      expect(classes[prevSizeClass]).toBeTruthy();
      component.size = '2x';
      component.updateFontIconClasses();
      expect(classes[prevSizeClass]).toBeFalsy();
      expect(classes[`${component.font}-${component.size}`]).toBeTruthy();
      expect(component.prevSizeClass).toBe(`${component.font}-${component.size}`);
    });

    it('should remove when fixed is set to false and add when set to true', () => {
      const classes = fixture.debugElement.classes;
      expect(component.fixed).toBeFalsy();
      component.font = 'fa';
      component.fixed = true;
      component.updateFontIconClasses();
      expect(classes['fa-fw']).toBeTruthy();
      expect(component.prevFixedWidthStatus).toBeTruthy();
      component.fixed = false;
      component.updateFontIconClasses();
      expect(classes['fa-fw']).toBeFalsy();
      expect(component.prevFixedWidthStatus).toBeFalsy();
    });
  });

});

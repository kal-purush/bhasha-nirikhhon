import {TestBed, async} from '@angular/core/testing';
import {KeybindingsDirective} from './keybindings.directive';

describe('KeybindingsDirective', () => {
  let directive: KeybindingsDirective;
  let mockElement, mockMousetrap, mockMousetrapStatic;

  beforeEach(() => {
    mockElement = {};
    mockMousetrap = jasmine.createSpy('Mousetrap');
    mockMousetrapStatic = jasmine.createSpyObj('MousetrapStatic', ['bind', 'unbind']);
    mockMousetrap.and.returnValue(mockMousetrapStatic);
    directive = new KeybindingsDirective(mockElement, mockMousetrap);
    directive.ehKeybindings = {
      'a': jasmine.createSpy('a callback'),
      'b': jasmine.createSpy('b callback')
    };
  });

  describe('.ngAfterViewInit', () => {
    it('should bind the keybindings to the callbacks', () => {
      directive.ngAfterViewInit();
      expect(mockMousetrapStatic.bind).toHaveBeenCalledWith('a', directive.ehKeybindings['a']);
      expect(mockMousetrapStatic.bind).toHaveBeenCalledWith('b', directive.ehKeybindings['b']);
    });
  });

  describe('.ngOnDestroy', () => {
    it('should unbind the keybindings', () => {
      directive.ngAfterViewInit();
      directive.ngOnDestroy();
      expect(mockMousetrapStatic.unbind).toHaveBeenCalledWith('a');
      expect(mockMousetrapStatic.unbind).toHaveBeenCalledWith('b');
    });
  });

});
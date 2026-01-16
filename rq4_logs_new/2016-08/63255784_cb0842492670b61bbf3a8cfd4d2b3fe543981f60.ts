import {FormControl} from '@angular/forms';

import {CmCollectionValidators} from './cm-collection.validator';


describe('CmCollectionValidators', () => {


  describe('at least one item', () => {

    it('should error if there is no selection', () => {
      expect(CmCollectionValidators.atLeastOneItemSelected(new FormControl([])))
        .toEqual({'atLeastSelectedItems': {'requiredCount': 1, 'actualCount': 0}});
    });

    it('should not if there is one selection', () => {
      expect(CmCollectionValidators.atLeastOneItemSelected(new FormControl(['x'])))
        .toEqual(null);
    });

  });

  describe('more than one item', () => {

    it('should error if there is no selection', () => {
      expect(CmCollectionValidators.atLeastSelectedItems(1)(new FormControl([])))
        .toEqual({'atLeastSelectedItems': {'requiredCount': 1, 'actualCount': 0}});
    });

    it('should not if there is one selection', () => {
      expect(CmCollectionValidators.atLeastSelectedItems(1)(new FormControl(['x'])))
        .toEqual(null);
    });

  });


});
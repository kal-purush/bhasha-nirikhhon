import { Categories } from './categories';

describe('Categories', () => {
  describe('of key', () => {
    it('should return single category', () => {
      expect(Categories.ofKey('amenity').key).toEqual('amenity');
    });
  });

  describe('calculates query', () => {
    it('should return single osm category for a main category', () => {
      expect(Categories.query('amenity')).toEqual('"amenity"');
    });

    it("should return a query with '=' for a single value", () => {
      expect(Categories.query('bank')).toEqual('"amenity"="bank"');
    });

    it("should return a query with '~' for multiple values", () => {
      expect(Categories.query('bowling')).toEqual('"sport"~"bowling|10pin|9pin"');
    });

    it("should return a query for category 'all'", () => {
      expect(Categories.query('all')).toEqual('~"^amenity|^historic|^leisure|^man_made|^shop|^sport|^tourism"~"."');
    });
  });

  describe('calculates category', () => {
    it('should return no category, when the value is unknown', () => {
      expect(Categories.category('amenity', 'unknown')).toBeUndefined();
    });

    it('should return category with single value', () => {
      expect(Categories.category('amenity', 'hunting_stand')).toEqual(Categories.ofKey('hunting'));
    });

    it('should return category with multiple values', () => {
      expect(Categories.category('amenity', 'fast_food')).toEqual(Categories.ofKey('restaurant'));
      expect(Categories.category('amenity', 'restaurant')).toEqual(Categories.ofKey('restaurant'));
    });
  });

  describe('calculates belongsTo', () => {
    it('should return true, if given category is in the list', () => {
      expect(Categories.belongsTo('restaurant', ['castle', 'restaurant', 'parking'])).toBeTruthy();
    });

    it('should return false, if given category is not in the list', () => {
      expect(Categories.belongsTo('restaurant', ['castle', 'parking'])).toBeFalsy();
    });

    it('should return true, if given category is a main category of an item in the list', () => {
      expect(Categories.belongsTo('amenity', ['castle', 'restaurant', 'lighthouse'])).toBeTruthy();
    });

    it('should return false, if given category is not a main category of an item in the list', () => {
      expect(Categories.belongsTo('amenity', ['castle', 'lighthouse'])).toBeFalsy();
    });
  });
});
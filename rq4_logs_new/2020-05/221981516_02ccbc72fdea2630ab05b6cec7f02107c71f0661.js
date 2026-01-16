const Chance = require('chance');
const schemas = require('../reference');

describe('Reference schema unit tests', () => {
  describe('#create', () => {
    const chance = new Chance();

    it('should be accepted when we send a valid schema', () => {
      const reference = {
        body: {
          description: chance.string({ length: 30 }),
          citation: chance.string({ length: 200 }),
          year: chance.year(),
          bibtex: chance.string({ alpha: true }),
        },
      };

      const { error } = schemas.createReference.validate(reference);
      expect(error).toEqual(undefined);
    });

    it('should return error if citation is not sent', () => {
      const reference = {
        body: {
          description: chance.string({ length: 30 }),
          year: chance.year(),
          bibtex: chance.string({ alpha: true }),
        },
      };

      const { error } = schemas.createReference.validate(reference);
      expect(error.message).toEqual('"body.citation" is required');
    });

    it('should be accepted when we not send year (it is optional)', () => {
      const reference = {
        body: {
          description: chance.string({ length: 30 }),
          citation: chance.string({ length: 200 }),
          bibtex: chance.string({ alpha: true }),
        },
      };

      const { error } = schemas.createReference.validate(reference);
      expect(error).toEqual(undefined);
    });

    it('should return error if bibtex is not sent', () => {
      const reference = {
        body: {
          description: chance.string({ length: 30 }),
          citation: chance.string({ length: 200 }),
          year: chance.year(),
        },
      };

      const { error } = schemas.createReference.validate(reference);
      expect(error.message).toEqual('"body.bibtex" is required');
    });
  });
});
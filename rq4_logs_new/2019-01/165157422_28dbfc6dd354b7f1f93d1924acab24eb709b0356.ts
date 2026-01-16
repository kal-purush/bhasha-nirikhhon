import {pupaTag} from './pupa-tag';

test('select * from foo where id = 123', () => {
  const selectById = pupaTag<{id: number}>`select * from foo where id = {id}`;
  expect(selectById({id: 123})).toBe('select * from foo where id = 123');
});

describe('Separation', () => {
  describe('select * from foo where id = 123', () => {
    // tslint:disable-next-line:no-invalid-template-strings
    test('${whereBy}', () => {
      const whereBy = pupaTag<{id: number}>`where id = {id}`;
      const selectById = pupaTag<{id: number}>`select * from foo ${whereBy}`;
      expect(selectById({id: 123})).toBe('select * from foo where id = 123');
    });

    // tslint:disable-next-line:no-invalid-template-strings
    test('${whereBy()}', () => {
      const whereBy = pupaTag<{id: number}>`where id = {id}`;
      const selectById = pupaTag<{id: number}>`select * from foo ${whereBy({
        id: 123
      })}`;
      expect(selectById()).toBe('select * from foo where id = 123');
    });
  });
});
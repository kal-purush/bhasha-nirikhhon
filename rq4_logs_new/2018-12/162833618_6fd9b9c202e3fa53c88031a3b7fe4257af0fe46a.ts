import {Amida} from './amida';

interface BehaviorObject {
  size: string;
}

const theme = {
  size: 123,
};

const wm = new WeakMap();
wm.set(theme, theme);

test('basis', () => {
  const amida = new Amida<BehaviorObject>({
    size: into => [
      into((v: string) => v).if.string(),
      into((num: number) =>
        Math.round(num) === num ? `${num}px` : `${num}em`,
      ).if.number(),
      into((obj: BehaviorObject) => `${obj.size}px`).if.object(),
      into((wm: WeakMap<object, BehaviorObject>) => {
        return `${wm.get(theme)!.size}px`;
      }).if.weakmap(),
    ],
  });

  const obj = amida.from(theme);
  expect(obj.size).toBe('123px');

  obj.size = '100%';
  expect(obj.size).toBe('100%');

  (obj.size as unknown) = theme;
  expect(obj.size).toBe('123px');

  (obj.size as unknown) = 999;
  expect(obj.size).toBe('999px');

  (obj.size as unknown) = 2.1;
  expect(obj.size).toBe('2.1em');

  (obj.size as unknown) = wm;
  expect(obj.size).toBe('123px');
});

test('throw', () => {
  const amida = new Amida<BehaviorObject>({
    size: into => [
      into((num: number) =>
        Math.round(num) === num ? `${num}px` : `${num}em`,
      ).if.number(),
      into((obj: BehaviorObject) => `${obj.size}px`).if.object(),
      into((wm: WeakMap<object, BehaviorObject>) => {
        return `${wm.get(theme)!.size}px`;
      }).if.weakmap(),
    ],
  });

  expect(() => {
    const obj = amida.from(theme);
    obj.size = '100%';
  }).toThrow();
});
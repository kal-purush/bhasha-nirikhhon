import { day20Part1, flip, inputToTiles, rotate, Tile } from './day20';
import { getInputNestedArrayString } from './tools';

describe('day20', () => {
  test('inputTos', () => {
    expect(inputToTiles([['Tile 3079:', '#.#.', '.#..', '..#.', '####']])).toEqual([
      { Cells: ['#.#.', '.#..', '..#.', '####'], ID: 3079 },
      { Cells: ['...#', '#.##', '.#.#', '#..#'], ID: 3079 },
      { Cells: ['####', '.#..', '..#.', '.#.#'], ID: 3079 },
      { Cells: ['#..#', '#.#.', '##.#', '#...'], ID: 3079 },
      { Cells: ['.#.#', '..#.', '.#..', '####'], ID: 3079 },
      { Cells: ['#..#', '.#.#', '#.##', '...#'], ID: 3079 },
      { Cells: ['####', '..#.', '.#..', '#.#.'], ID: 3079 },
      { Cells: ['#...', '##.#', '#.#.', '#..#'], ID: 3079 },
    ]);
    expect(inputToTiles(getInputNestedArrayString(20, 1)).length).toEqual(1152);
  });
  test('flip', () => {
    expect(
      flip([
        '..##.#..#.',
        '##..#.....',
        '#...##..#.',
        '####.#...#',
        '##.##.###.',
        '##...#.###',
        '.#.#.#..##',
        '..#....#..',
        '###...#.#.',
        '..###..###',
      ]),
    ).toEqual([
      '.#..#.##..',
      '.....#..##',
      '.#..##...#',
      '#...#.####',
      '.###.##.##',
      '###.#...##',
      '##..#.#.#.',
      '..#....#..',
      '.#.#...###',
      '###..###..',
    ]);
  });
  test('rotate', () => {
    expect(rotate(['####', '###.', '####', '####'])).toEqual(['#.##', '####', '####', '####']);
  });
  test('Tile', () => {
    const a = new Tile(1, ['#.#.#', '#...#', '#...#', '#.#.#']);
    const b = new Tile(1, ['#.#.#', '#...#', '#...#', '#.#.#']);
    expect(a.canBelow(b)).toBeTruthy();
    expect(a.canRight(b)).toBeTruthy();
    const c = new Tile(1, ['#.#.#', '#...#', '#...#', '#.#.#']);
    const d = new Tile(1, ['.....', '#....', '#....', '#.#..']);
    expect(c.canBelow(d)).toBeFalsy();
    expect(c.canRight(d)).toBeFalsy();
    expect(d.canBelow(c)).toBeFalsy();
    expect(d.canRight(c)).toBeFalsy();
  });
  test('day20Part1', () => {
    const tiles = inputToTiles(getInputNestedArrayString(20, 1));
    expect(day20Part1(tiles)).toEqual(4006801655873);
  });
});
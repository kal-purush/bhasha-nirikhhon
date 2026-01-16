import { PuzzleResult, enableVerbose, formatResponse, getPuzzleInput, isVerbose, verboseLog } from '../utils';

const dayNumber = 5;

export async function solveDayFive(): Promise<PuzzleResult> {
  const input = await getPuzzleInput(dayNumber, '1');
  const sections = input.split('\n\n');
  let partOne = 0;
  let partTwo = 0;
  //enableVerbose();

  interface Map {
    sourceStart: number,
    destStart: number,
    length: number,
  }

  const getMappedValue = (value: number, maps: Map[]): number => {

    let mapped = value;
    for (const map of maps) {
      const offset = value - map.sourceStart;

      if (offset >= 0 && offset < map.length) {
        mapped = map.destStart + offset;
        break;
      }
    }

    return mapped;
  }

  const findLocation = (seed: number, maps: Map[][]): number => {
    const mapHistory = [seed];
    let current = seed;
    for (const mapping of maps) {
      current = getMappedValue(current, mapping);
      mapHistory.push(current);
    }

    verboseLog(mapHistory);

    return current;
  }

  const parseMaps = (input: string): Map[] => {
    const maps: Map[] = [];

    for (const l of input.split('\n').slice(1)) {
      const [destStart, sourceStart, length] = l.split(' ').map(n => parseInt(n));
      maps.push({
        sourceStart, destStart, length
      });
    }
    return maps;
  }

  const seeds: number[] = sections[0]
    .split(' ')
    .map(s => parseInt(s))
    .filter(s => !isNaN(s));

  verboseLog(seeds);

  const maps = sections.slice(1).map(s => parseMaps(s));


  if (isVerbose()) {
    for (const m of maps) {
      console.log(m);
      console.log();
    }
  }

  // Part One
  const seedLocations = seeds.map(seed => findLocation(seed, maps));

  partOne = seedLocations.reduce((min, location) => Math.min(min, location), Infinity);

  // Part Two
  const seedRanges: { start: number, length: number }[] = [];
  for (let i = 0; i < seeds.length; i += 2) {
    seedRanges.push({ start: seeds[i], length: seeds[i + 1] });
  }

  const orderedRanges = seedRanges.sort((a, b) => b.start - a.start);

  partTwo = Infinity;
  let seed = 0;
  for (const range of orderedRanges) {
    const { start, length } = range;
    seed = Math.max(seed, start);

    while (seed < (start + length)) {
      const location = findLocation(seed, maps);
      partTwo = Math.min(partTwo, location);
      seed++;      
    }
  }


  return { partOne, partTwo };

}
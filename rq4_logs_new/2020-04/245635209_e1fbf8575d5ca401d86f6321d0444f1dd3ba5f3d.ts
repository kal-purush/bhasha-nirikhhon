import { CsvFileReader } from './CsvFileReader';
import { dateStringToDate } from '../utils';
import { MatchResult } from '../MatchResult';

interface DataReader {
  data: string[][];
  read(): void;
}

type MatchDataTuple = [
  Date,
  string,
  string,
  number,
  number,
  MatchResult,
  string
];

export class MatchReader {
  matchs: MatchDataTuple[] = [];

  constructor(public reader: DataReader) {}

  load(): void {
    this.reader.read();
    this.matchs = this.reader.data.map(this.mapFootballRow);
  }

  mapFootballRow(row: string[]): MatchDataTuple {
    return [
      dateStringToDate(row[0]),
      row[1],
      row[2],
      +row[3],
      +row[4],
      row[5] as MatchResult,
      row[6],
    ];
  }
}
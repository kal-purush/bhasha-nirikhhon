import { MatchResult } from './MatchResult';
import { MatchReader, MatchDataTuple } from './inheritance/MatchReader';

const csvFileName = 'football_matchs.csv';
const logPerspolisReport = (perspolisData: MatchDataTuple[]): void => {
  let perspolisWinsH = 0;
  let perspolisWinsA = 0;

  for (let match of perspolisData) {
    if (match[1] === 'Perspolis' && match[5] === MatchResult.HomeWins) {
      perspolisWinsH++;
    } else if (match[2] === 'Perspolis' && match[5] === MatchResult.AwayWins) {
      perspolisWinsA++;
    }
  }

  console.log(`Perspolis wins ${perspolisWinsH} games at home`);
  console.log(`Perspolis wins ${perspolisWinsA} games at away`);
  console.log(`Perspolis wins ${perspolisWinsA + perspolisWinsH} games`);
};

// Inheritance Method
const footballMatchsReader = new MatchReader(csvFileName);
footballMatchsReader.read();

logPerspolisReport(footballMatchsReader.data);

// =======================

// Interface method
import { MatchReader as MatchReaderInterface } from './interface/MatchReader';
import { CsvFileReader as CsvFileReaderInterface } from './interface/CsvFileReader';

// Creating an object that satisfies the 'DataReader'interface
const csvFileReaderInterface = new CsvFileReaderInterface(csvFileName);

// Creating an instance of MatchReader and passing something in satisfying the 'DataReader' interface
const matchReader = new MatchReaderInterface(csvFileReaderInterface);
matchReader.load();

logPerspolisReport(matchReader.matchs);
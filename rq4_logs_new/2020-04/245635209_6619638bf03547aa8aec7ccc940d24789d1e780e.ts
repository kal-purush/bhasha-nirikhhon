import { MatchDataTuple } from '../MatchDataTuple';
import { WinsAnalysis } from '../analyzers/WinsAnalysis';
import { ConsoleReport } from '../reportTargets/ConsoleReport';
import { HtmlReport } from '../reportTargets/HtmlReport';

export interface Analyzer {
  run(matchs: MatchDataTuple[]): string;
}

export interface OutputTarget {
  print(report: string): void;
}

export class Summery {
  static winAnalysisWithConsolwReport(team: string): Summery {
    return new Summery(new WinsAnalysis(team), new ConsoleReport());
  }
  static winAnalysisWithHtmlwReport(
    team: string,
    outputFilename: string
  ): Summery {
    return new Summery(new WinsAnalysis(team), new HtmlReport(outputFilename));
  }
  constructor(public analyzer: Analyzer, public outputTarget: OutputTarget) {}

  buildAndReport(matchs: MatchDataTuple[]): void {
    const output = this.analyzer.run(matchs);
    this.outputTarget.print(output);
  }
}
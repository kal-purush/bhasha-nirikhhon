export interface StartOfSourceMap {
  file?: string;
  sourceRoot?: string;
}

export interface RawSourceMap extends StartOfSourceMap {
  version: string;
  sources: Array<string>;
  names: Array<string>;
  sourcesContent?: string[];
  mappings: string;
}

export interface Position {
  line: number;
  column: number;
}

export interface MappedPosition extends Position {
  source: string;
  name?: string;
}

export interface MappingItem {
  source: string;
  generatedLine: number;
  generatedColumn: number;
  originalLine: number;
  originalColumn: number;
  name: string;
}

export class SourceMapConsumer {
  public static GENERATED_ORDER: number;
  public static ORIGINAL_ORDER: number;

  constructor(rawSourceMap: RawSourceMap);
  public originalPositionFor(generatedPosition: Position): MappedPosition;
  public generatedPositionFor(originalPosition: MappedPosition): Position;
  public sourceContentFor(source: string): string;
  public eachMapping(callback: (mapping: MappingItem) => void, context?: any, order?: number): void;
}
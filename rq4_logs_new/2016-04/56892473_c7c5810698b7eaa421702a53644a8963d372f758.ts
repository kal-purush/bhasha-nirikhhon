declare class Mime {
  types: {
    [extension: string]: string;
  }

  extensions: {
    [extension: string]: string;
  }

  define (map: Map): void;
  load (filename: string): void;
  lookup (path: string, fallback?: string): string;
  extension (mimeType: string): string;
}

declare interface Map {
  [type: string]: string[];
}

declare var mime: Mime & {
  default_type: string;
  charsets: {
    lookup (mimeType: string, fallback?: string): string;
  }
  Mime: typeof Mime;
}

export = mime;
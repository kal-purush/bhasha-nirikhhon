export type BreakpointsPoints = {
  [ key: string ]: number,
};

export type BreakpointsNames = string | string[];

export type BreakpointsQueries = Array<{ name: string, screen: number, media: MediaQueryList }>;
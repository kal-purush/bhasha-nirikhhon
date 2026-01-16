export const majorChordIdentifiers = ['M', 'maj', 'Δ'];
export const minorChordIdentifiers = ['m', 'min', '-'];
export const diminishedChordIdentifiers = ['dim', '°'];
export const halfDiminishedChordIdentifiers = ['ø', 'm7b5'];
export const augmentedChordIdentifiers = ['aug', '+'];

export const chordSymbols = [
  ...majorChordIdentifiers,
  ...minorChordIdentifiers,
  ...diminishedChordIdentifiers,
  ...halfDiminishedChordIdentifiers,
  ...augmentedChordIdentifiers,
];

export const chordQualities = [
  'major',
  'minor',
  'augmented',
  'diminished',
  'half-diminished',
  'dominant',
  'minor-major',
] as const;

export type ChordQuality = (typeof chordQualities)[number];
export function isChordQuality(input: string): input is ChordQuality {
  return !!chordQualities.find((val) => input === val);
}

// Chord Type
export const chordTypes = [
  'second',
  'triad',
  'fourth',
  'fifth',
  'sixth',
  'seventh',
  'ninth',
  'eleventh',
  'thirteenth',
] as const;
export type ChordType = (typeof chordTypes)[number];

export const romanNumerals = [
  'I',
  'II',
  'III',
  'IV',
  'V',
  'VI',
  'VII',
] as const;
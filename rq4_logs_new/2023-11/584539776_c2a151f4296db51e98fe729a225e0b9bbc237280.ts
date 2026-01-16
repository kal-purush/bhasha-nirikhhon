import { describe, expect, it } from 'vitest';
import { isSupportedChord } from './isSupportedChord.js';

describe.only('isSupportedChord', () => {
  const supportedChords = [
    'C',
    'Cm',
    'Cdim',
    'Caug',
    'C#',
    'Bb',
    'Cadd4',
    'Cadd2',
    'Cadd9',
    'Cadd11',
    'Cadd13',
    'Csus',
    'Csus2',
    'Csus9',
    'C2',
    'C4',
    'C5',
    'C6',
    'C7',
    'C9',
    'C11',
    'C13',
    'Cm7',
    'Bb7',
    'F#m7',
    'Cm7b5',
  ];
  it.each(supportedChords)('considers %s valid', (chordName) => {
    expect(isSupportedChord(chordName)).toBe(true);
  });

  const invalidChords = [
    'Ce',
    'Cx',
    'Z',
    'CZ',
    'C10',
    'B8',
    'Cadd43',
    'Caddx',
    'Cadd10',
    'Cadd1',
    'Cadd3',
    'Csusb',
    'Csus0',
    'Csus42',
    'Csus11',
    'Bb1',
    'Cm7b8',
    'Cdimm',
    'F#augg',
    'F#mm',
  ];
  it.each(invalidChords)('considers %s invalid', (chordName) => {
    expect(isSupportedChord(chordName)).toBe(false);
  });
});
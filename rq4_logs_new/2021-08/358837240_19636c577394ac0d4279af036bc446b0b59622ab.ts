import * as sanitization from '../sanitization';

test('test sanitization function', () => {
  expect(sanitization.formatName('adrian')).toBe('Adrian');
  expect(sanitization.formatName('adrian timotheus')).toBe('Adrian Timotheus');

  expect(sanitization.formatTelp('087123456789')).toBe('087123456789');
  expect(sanitization.formatTelp('+6287123456789')).toBe('087123456789');

  expect(sanitization.formatSekolah('sma kristen 1 bandung')).toBe('SMAK 1 Bandung');
  expect(sanitization.formatSekolah('sma negeri 1 jakarta')).toBe('SMAN 1 Jakarta');
  expect(sanitization.formatSekolah('sma swasta 3 surabaya')).toBe('SMAS 3 Surabaya');

  expect(sanitization.formatGereja('gki maulana yusuf')).toBe('GKI Maulana Yusuf');
  expect(sanitization.formatGereja('hkbp bandung timur')).toBe('HKBP Bandung Timur');
});
import { ConvertNaNPipe } from './convert-nan.pipe';

describe('ConvertNaNPipe', () => {
  const pipe = new ConvertNaNPipe();

  test('should convert NaN to 0.0', () => {
    expect(pipe.transform(NaN)).toEqual(0.0);
    expect(pipe.transform(undefined)).toEqual(0.0);
  });

  test('should not alter a normal number', () => {
    expect(pipe.transform(-1089.79)).toEqual(-1089.79);
    expect(pipe.transform(-1.0)).toEqual(-1.0);
    expect(pipe.transform(-8888)).toEqual(-8888);
    expect(pipe.transform(0.0)).toEqual(0.0);
    expect(pipe.transform(0)).toEqual(0);
    expect(pipe.transform(10700)).toEqual(10700);
    expect(pipe.transform(9212.0)).toEqual(9212.0);
    expect(pipe.transform(100.6)).toEqual(100.6);
  });
});
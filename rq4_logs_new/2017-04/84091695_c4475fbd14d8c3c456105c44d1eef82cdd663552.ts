import { DurationPipe } from './duration.pipe';

fdescribe('DurationPipe', () => {
  it('should 59 be "59 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(59)).toBe('59 min');
  });
  it('should 120 be "2 h 00 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(120)).toBe('2 h 00 min');
  });
  it('should 61 be "1 h 01 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(61)).toBe('1 h 01 min');
  });
  it('should 60 be "1 h 00 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(60)).toBe('1 h 00 min');
  });
  it('should 122 be "2 h 02 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(122)).toBe('2 h 02 min');
  });
  it('should 152 be "2 h 32 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(152)).toBe('2 h 32 min');
  });
  it('should 9 be "09 min"', () => {
    const pipe = new DurationPipe();
    expect(pipe.transform(9)).toBe('09 min');
  });
});
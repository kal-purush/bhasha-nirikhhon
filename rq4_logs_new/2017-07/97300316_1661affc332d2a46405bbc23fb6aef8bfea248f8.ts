import { AllButNotGitKeepSanitizeStrategy } from './AllButNotGitKeepSanitizeStrategy';

describe('AllButNotGitKeepSanitizeStrategy', () => {
  it('should return only json files from given list of files', () => {
    const sStrategy = new AllButNotGitKeepSanitizeStrategy();

    expect(sStrategy.sanitize([])).toEqual([]);
    expect(sStrategy.sanitize([null, undefined, ''])).toEqual([]);

    expect(
      sStrategy.sanitize(['.gitkeep', 'en.json', 'pl.json', 'lol.bin', 'without'])
    ).toEqual(['en.json', 'pl.json', 'lol.bin', 'without']);
  });
});